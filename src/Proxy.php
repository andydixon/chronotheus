<?php

namespace Andydixon\Chronotheus;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;

/**
 * Proxy Class (Chronotheus)
 *
 * Okay, so this is rewrite 6(?) and is basically a middle-man for Prometheus API calls. Why you may ask:
 * it intercepts your queries, does some funky timestamp maths, and then splats out
 * extra data slices so you can see "what happened now" vs "what happened 7, 14, 21, 28 days ago"
 * (we call that 'chrono_timeframe' BTW).
 *
 * What it actually does:
 *   - Listens at /<host>_<port>/api/v1/... and figures out if you want:
 *     • Instant query ('/query') or  
 *     • Range query ('/query_range') or  
 *     • Labels ('/labels') or  
 *     • Label values ('/label/{name}/values') or  
 *     • Anything else (then it just proxies straight through, no fuss).
 *
 * Historical magic:
 *   - Offsets: now, 7d, 14d, 21d, 28d (that’s 0, 7×24h, 14×24h, etc.)
 *   - Shifts your time window back by each offset, fetches data in parallel,
 *   - Shifts those timestamps forward so it "lines up" with today,
 *   - Tags each series with 'chrono_timeframe="current|7days|14days|21days|28days"'.
 *
 * Bonus series (because why not?):
 *   - 'lastMonthAverage': per-metric minute-by-minute average of the four historical slices,
 *   - 'compareAgainstLast28': raw difference (current minus average),
 *   - 'percentCompareAgainstLast28': percent difference ((current−avg)/avg×100).
 *
 * Also supports:
 *   - A '_command="whatever"' flag you can sneak into your PromQL and it’ll
 *     carry that through on every returned series (like a little post-it note), but is also stored
 *	   in $this->command so we could switch things off and on at the drop of a hat.
 *   - JSON or form-encoded POST bodies or plain GET params,
 *   - Smart stripping of those synthetic labels before contacting real Prometheus
 *     (and cleaning up any leftover commas—no '{,foo="bar",}' crap that kept me up late at night).
 *
 * Under the hood:
 *   - GuzzleHttp\Client for all the HTTP bits,
 *   - PHP’s built-in parse_str / json_decode / strtotime / array magic to manipulate stuff,
 *   - A bunch of helper methods to build query strings, dedupe series by label signature,
 *     bucket & average points, subtract, percentify, etc.
 *
 * TLDR: It’s the lazy persons way of getting trend analysis data, makes five copies with different
 * time offsets, glues them back together, and then throws in two more plots for comparison,
 * all while quietly logging any upstream errors so I can finally sleep at night without dreaming of data.
 */


class Proxy
{
    // Offsets in seconds: now, 7d, 14d, 21d, 28d
    private array $offsets = [
        0,
        7  * 24 * 3600,
        14 * 24 * 3600,
        21 * 24 * 3600,
        28 * 24 * 3600,
    ];

    // Labels we'll inject under "chrono_timeframe"
    private array $timeframes = [
        'current',
        '7days',
        '14days',
        '21days',
        '28days',
    ];

    /**
     * Main router: peel off /<host>_<port>/, then dispatch.
     */
    public function handle(string $uri, array $server, ?string $body = null): void
    {
        $path = parse_url($uri, PHP_URL_PATH) ?: '/';
        if (!preg_match('#^/([^_/]+)_(\d+)(/.*)?$#', $path, $m)) {
            http_response_code(400);
            header('Content-Type: application/json');
            echo json_encode(['status'=>'error','error'=>'Invalid target prefix']);
            return;
        }
        [, $host, $port, $suffix] = $m;
        $suffix   = $suffix ?: '/';
        $upstream = "http://{$host}:{$port}";
        $method   = strtoupper($server['REQUEST_METHOD']);

        // dispatch by path and method
        if (preg_match('#^/api/v1/query$#', $suffix) && in_array($method, ['GET','POST'], true)) {
            $this->handleQuery($upstream, '/api/v1/query', $server, $body);

        } elseif (preg_match('#^/api/v1/query_range$#', $suffix) && in_array($method, ['GET','POST'], true)) {
            $this->handleQueryRange($upstream, '/api/v1/query_range', $server, $body);

        } elseif (in_array($method, ['GET','POST'], true) && preg_match('#^/api/v1/labels/?$#', $suffix)) {
            $this->handleLabels($upstream, '/api/v1/labels');

        } elseif (in_array($method, ['GET','POST'], true)
              && preg_match('#^/api/v1/label/([^/]+)/values/?$#', $suffix, $nm)
        ) {
            $this->handleLabelValues($upstream, "/api/v1/label/{$nm[1]}/values", $nm[1]);

        } else {
            // any other path → raw proxy
            $this->forward($upstream . $suffix, $method, $body);
        }
    }

    /**
     * Handle instant query at /api/v1/query
     */
    private function handleQuery(string $upstream, string $path, array $server, ?string $body): void
    {
        $method = strtoupper($server['REQUEST_METHOD']);
        $params = $this->parseClientParams();

        // catch any chrono_timeframe="..." or _command="..." user asked for
        $requestedTf = null;
        if (!empty($params['query'])
            && preg_match('/\bchrono_timeframe="([^"]+)"/', $params['query'], $m1)
        ) {
            $requestedTf = $m1[1];   // like "7days" or "lastMonthAverage"
        }
        $command = null;
        if (!empty($params['query'])
            && preg_match('/\b_command="([^"]+)"/', $params['query'], $m2)
        ) {
            $command = $m2[1];       // any flag you want to carry
        }

        // strip those out before talking to Prometheus
        $this->stripLabelFromParam($params, 'query', 'chrono_timeframe');
        $this->stripLabelFromParam($params, 'query', 'command');

        $client    = new Client();
        $allSeries = [];

        // loop through now,7d,14d,21d,28d
        foreach ($this->offsets as $i => $offset) {
            $tfLabel        = $this->timeframes[$i];
            // adjust the time backwards by offset
            $tsEpoch        = $this->parseTime($params['time'] ?? null);
            $params['time'] = $tsEpoch - $offset;

            try {
                $resp = $client->request('GET', $upstream . $path, [
                    'query' => $params,
                ]);
            } catch (GuzzleException $e) {
                error_log("[Chrono][Upstream ERROR] {$method} {$upstream}{$path}?{$this->buildQueryString($params)} → ".$e->getMessage());
                http_response_code(502);
                header('Content-Type: application/json');
                echo json_encode(['status'=>'error','error'=>'Upstream request failed']);
                return;
            }

            $data = json_decode($resp->getBody()->getContents(), true) ?: [];
            if (empty($data['data']['result']) || !is_array($data['data']['result'])) {
                // no results? meh, move on
                continue;
            }

            // shift timestamps forward & tag
            foreach ($data['data']['result'] as $series) {
                [$ts, $val] = $series['value'];
                $series['value'] = [
                    $ts + $offset,
                    (string)$val, // preserve JSON-string
                ];
                $series['metric']['chrono_timeframe'] = $tfLabel;
                if ($command !== null) {
                    $series['metric']['_command'] = $command;
                }
                $allSeries[] = $series;
            }
        }

        // dedupe by metric signature (excluding synthetic labels)
        $merged = $this->dedupeSeries($allSeries);

        // if they asked for one timeframe only, just filter and bail early
        if ($requestedTf !== null) {
            $filtered = array_values(array_filter(
                $merged,
                fn($s) => ($s['metric']['chrono_timeframe'] ?? '') === $requestedTf
            ));
            header('Content-Type: application/json');
            echo json_encode([
                'status' => 'success',
                'data'   => [
                    'resultType' => 'vector',
                    'result'     => $filtered,
                ],
            ]);
            return;
        }

        // build lastMonthAverage per signature
        $avgList = $this->buildLastMonthAverage($merged, false);
        foreach ($avgList as $avg) {
            if ($command !== null) {
                $avg['metric']['_command'] = $command;
            }
            $merged[] = $avg;
        }

        // build compareAgainstLast28 (raw diff)
        $curBySig = [];
        foreach ($merged as $s) {
            if (($s['metric']['chrono_timeframe'] ?? '') === 'current') {
                $curBySig[$this->signature($s['metric'])] = $s;
            }
        }
        $avgBySig = [];
        foreach ($avgList as $s) {
            $avgBySig[$this->signature($s['metric'])] = $s;
        }
        foreach ($curBySig as $sig => $curSeries) {
            if (!isset($avgBySig[$sig])) {
                continue;
            }
            $avgSeries = $avgBySig[$sig];
            $metric    = $curSeries['metric'];
            $metric['chrono_timeframe'] = 'compareAgainstLast28';
            if ($command !== null) {
                $metric['_command'] = $command;
            }
            [$tsc, $vc] = $curSeries['value'];
            [,   $va]   = $avgSeries['value'];
            $diff       = (float)$vc - (float)$va;
            $merged[]   = [
                'metric' => $metric,
                'value'  => [(int)$tsc, (string)$diff],
            ];
        }

        // now percentCompareAgainstLast28 ((cur-avg)/avg*100)
        $percentList = [];
        foreach ($curBySig as $sig => $curSeries) {
            if (!isset($avgBySig[$sig])) {
                continue;
            }
            $avgSeries = $avgBySig[$sig];
            $metric    = $curSeries['metric'];
            $metric['chrono_timeframe'] = 'percentCompareAgainstLast28';
            if ($command !== null) {
                $metric['_command'] = $command;
            }

            [$tsc, $vc] = $curSeries['value'];
            [,   $va]   = $avgSeries['value'];
            if ((float)$va != 0.0) {
                $pct = ((float)$vc - (float)$va) / (float)$va * 100;
            } else {
                // avoid divide by zero, ugh
                $pct = 0.0;
            }
            $percentList[] = [
                'metric' => $metric,
                'value'  => [(int)$tsc, (string)$pct],
            ];
        }
        foreach ($percentList as $p) {
            $merged[] = $p;
        }

        // done! return the lot
        header('Content-Type: application/json');
        echo json_encode([
            'status' => 'success',
            'data'   => [
                'resultType' => 'vector',
                'result'     => array_values($merged),
            ],
        ]);
    }

    /**
     * Handle /api/v1/query_range (range queries)
     */
    private function handleQueryRange(string $upstream, string $path, array $server, ?string $body): void
    {
        $method = strtoupper($server['REQUEST_METHOD']);
        $params = $this->parseClientParams();

        // grab any requested timeframe or command
        $requestedTf = null;
        if (!empty($params['query'])
            && preg_match('/\bchrono_timeframe="([^"]+)"/', $params['query'], $m1)
        ) {
            $requestedTf = $m1[1];
        }
        $command = null;
        if (!empty($params['query'])
            && preg_match('/\b_command="([^"]+)"/', $params['query'], $m2)
        ) {
            $command = $m2[1];
        }
        // strip them out
        $this->stripLabelFromParam($params, 'query', 'chrono_timeframe');
        $this->stripLabelFromParam($params, 'query', 'command');

        // ensure a step
        if (empty($params['step'])) {
            $params['step'] = 60;
        }

        $client    = new Client();
        $allSeries = [];

        // same loop over offsets
        foreach ($this->offsets as $i => $offset) {
            $tfLabel           = $this->timeframes[$i];
            $params['start'] = $this->parseTime($params['start'] ?? null) - $offset;
            $params['end']   = $this->parseTime($params['end']   ?? null) - $offset;

            try {
                $resp = $client->request('GET', $upstream . $path, [
                    'query' => $params,
                ]);
            } catch (GuzzleException $e) {
                error_log("[Chrono][Upstream ERROR] {$method} {$upstream}{$path}?{$this->buildQueryString($params)} → ".$e->getMessage());
                http_response_code(502);
                header('Content-Type: application/json');
                echo json_encode(['status'=>'error','error'=>'Upstream request failed']);
                return;
            }

            $data = json_decode($resp->getBody()->getContents(), true) ?: [];
            if (empty($data['data']['result']) || !is_array($data['data']['result'])) {
                continue;
            }

            // shift & tag each series
            foreach ($data['data']['result'] as $series) {
                $shifted = [];
                foreach ($series['values'] as [$ts, $val]) {
                    $shifted[] = [$ts + $offset, (string)$val];
                }
                $series['values']                  = $shifted;
                $series['metric']['chrono_timeframe'] = $tfLabel;
                if ($command !== null) {
                    $series['metric']['_command'] = $command;
                }
                $allSeries[] = $series;
            }
        }

        // dedupe
        $merged = $this->dedupeSeries($allSeries);

        // timeframe filter shortcut
        if ($requestedTf !== null) {
            $filtered = array_values(array_filter(
                $merged,
                fn($s) => ($s['metric']['chrono_timeframe'] ?? '') === $requestedTf
            ));
            header('Content-Type: application/json');
            echo json_encode([
                'status' => 'success',
                'data'   => [
                    'resultType'=>'matrix',
                    'result'    =>$filtered,
                ],
            ]);
            return;
        }

        // build averages
        $avgList = $this->buildLastMonthAverage($merged, true);
        foreach ($avgList as $avg) {
            if ($command !== null) {
                $avg['metric']['_command'] = $command;
            }
            $merged[] = $avg;
        }

        // raw diff
        $curBySig = [];
        foreach ($merged as $s) {
            if (($s['metric']['chrono_timeframe'] ?? '') === 'current') {
                $curBySig[$this->signature($s['metric'])] = $s;
            }
        }
        $avgBySig = [];
        foreach ($avgList as $s) {
            $avgBySig[$this->signature($s['metric'])] = $s;
        }
        foreach ($curBySig as $sig => $curSeries) {
            if (!isset($avgBySig[$sig])) {
                continue;
            }
            $avgSeries = $avgBySig[$sig];
            $metric    = $curSeries['metric'];
            $metric['chrono_timeframe'] = 'compareAgainstLast28';
            if ($command !== null) {
                $metric['_command'] = $command;
            }
            $avgMap = [];
            foreach ($avgSeries['values'] as [$ts, $v]) {
                $avgMap[$ts] = (float)$v;
            }
            $diffVals = [];
            foreach ($curSeries['values'] as [$ts, $v]) {
                if (isset($avgMap[$ts])) {
                    $delta = (float)$v - $avgMap[$ts];
                    $diffVals[] = [$ts, (string)$delta];
                }
            }
            $merged[] = [
                'metric' => $metric,
                'values' => $diffVals,
            ];
        }

        // percent diff
        $percentList = [];
        foreach ($curBySig as $sig => $curSeries) {
            if (!isset($avgBySig[$sig])) {
                continue;
            }
            $avgSeries = $avgBySig[$sig];
            $metric    = $curSeries['metric'];
            $metric['chrono_timeframe'] = 'percentCompareAgainstLast28';
            if ($command !== null) {
                $metric['_command'] = $command;
            }
            $avgMap = [];
            foreach ($avgSeries['values'] as [$ts, $v]) {
                $avgMap[$ts] = (float)$v;
            }
            $pctVals = [];
            foreach ($curSeries['values'] as [$ts, $v]) {
                if (!isset($avgMap[$ts]) || $avgMap[$ts] == 0.0) {
                    continue;
                }
                $pct = ((float)$v - $avgMap[$ts]) / $avgMap[$ts] * 100;
                $pctVals[] = [$ts, (string)$pct];
            }
            $percentList[] = [
                'metric' => $metric,
                'values' => $pctVals,
            ];
        }
        foreach ($percentList as $p) {
            $merged[] = $p;
        }

        // send it back
        header('Content-Type: application/json');
        echo json_encode([
            'status' => 'success',
            'data'   => [
                'resultType' => 'matrix',
                'result'     => array_values($merged),
            ],
        ]);
    }

    /**
     * Handle GET/POST /api/v1/labels — forward client filters, strip ours, then append.
     */
    private function handleLabels(string $upstream, string $path): void
    {
        $params = $this->parseClientParams();

        // strip any chrono_timeframe or command filters
        $this->stripLabelFromParam($params, 'match', 'chrono_timeframe');
        $this->stripLabelFromParam($params, 'match', 'command');

        // remap PHP’s match→match[] if needed
        if (isset($params['match']) && !isset($params['match[]'])) {
            $params['match[]'] = $params['match'];
            unset($params['match']);
        }

        $qs  = $this->buildQueryString($params);
        $url = "{$upstream}{$path}?{$qs}";

        try {
            $resp = (new Client())->request('GET', $url);
        } catch (GuzzleException $e) {
            error_log("[Chrono][Upstream ERROR] handleLabels → ".$e->getMessage());
            http_response_code(502);
            header('Content-Type: application/json');
            echo json_encode(['status'=>'error','error'=>'Upstream request failed']);
            return;
        }

        $data = json_decode($resp->getBody()->getContents(), true) ?: [];
        // ensure the data array
        if (!isset($data['data']) || !is_array($data['data'])) {
            $data['data'] = [];
            $data['status'] = 'success';
        }
        // append our label
        if (!in_array('chrono_timeframe', $data['data'], true)) {
            $data['data'][] = 'chrono_timeframe';
        }

        header('Content-Type: application/json');
        echo json_encode($data);
    }

    /**
     * Handle GET/POST /api/v1/label/{name}/values — similar to labels.
     */
    private function handleLabelValues(string $upstream, string $path, string $label): void
    {
        // synthetic label: list our timeframes + extras
        if ($label === 'chrono_timeframe') {
            header('Content-Type: application/json');
            echo json_encode([
                'status'=>'success',
                'data'=>array_merge(
                    $this->timeframes,
                    ['lastMonthAverage','compareAgainstLast28','percentCompareAgainstLast28']
                ),
            ]);
            return;
        }

        $params = $this->parseClientParams();
        $this->stripLabelFromParam($params, 'match', 'chrono_timeframe');
        $this->stripLabelFromParam($params, 'match', 'command');
        if (isset($params['match']) && !isset($params['match[]'])) {
            $params['match[]'] = $params['match'];
            unset($params['match']);
        }

        $qs  = $this->buildQueryString($params);
        $url = "{$upstream}{$path}?{$qs}";

        try {
            $resp = (new Client())->request('GET', $url);
        } catch (GuzzleException $e) {
            error_log("[Chrono][Upstream ERROR] handleLabelValues → ".$e->getMessage());
            http_response_code(502);
            header('Content-Type: application/json');
            echo json_encode(['status'=>'error','error'=>'Upstream request failed']);
            return;
        }

        $data = json_decode($resp->getBody()->getContents(), true) ?: [];
        header('Content-Type: application/json');
        echo json_encode($data);
    }

    /**
     * Raw proxy for anything else.
     */
    private function forward(string $url, string $method, ?string $body): void
    {
        $opts = $method === 'GET' ? ['query'=>$_GET] : ['body'=>$body];
        $resp = (new Client())->request($method, $url, $opts);
        header("Content-Type: {$resp->getHeaderLine('Content-Type')}");
        echo $resp->getBody()->getContents();
    }

    /**
     * Parse client params: GET → $_GET, POST JSON → body, POST form → parse_str
     */
    private function parseClientParams(): array
    {
        $method      = $_SERVER['REQUEST_METHOD'] ?? 'GET';
        $contentType = $_SERVER['CONTENT_TYPE']   ?? '';
        $rawBody     = file_get_contents('php://input');

        if ($method === 'POST') {
            if (stripos($contentType, 'application/json') !== false) {
                $d = json_decode($rawBody, true);
                return is_array($d) ? $d : [];
            }
            parse_str($rawBody, $params);
            return $params;
        }
        return $_GET;
    }

    /**
     * Strip any ,?label="value" from $params[$key], clean up commas.
     */
    private function stripLabelFromParam(array &$params, string $key, string $label): void
    {
        if (!isset($params[$key])) {
            return;
        }
        // remove the matcher
        $pattern = '/,?'.preg_quote($label,'/').'="[^"]*"/';
        $s = preg_replace($pattern, '', $params[$key]);
        // collapse repeated commas → single
        $s = preg_replace('/,+/', ',', $s);
        // remove comma after { or before }
        $s = preg_replace('/\{\s*,+/', '{', $s);
        $s = preg_replace('/,+\s*\}/', '}', $s);
        $params[$key] = $s;
    }

    /**
     * Build a query string, repeating keys for arrays: match[]=a&match[]=b
     */
    private function buildQueryString(array $params): string
    {
        $parts = [];
        foreach ($params as $k => $v) {
            if (is_array($v)) {
                $name = str_ends_with($k,'[]') ? $k : "{$k}[]";
                foreach ($v as $x) {
                    $parts[] = rawurlencode($name).'='.rawurlencode($x);
                }
            } else {
                $parts[] = rawurlencode($k).'='.rawurlencode($v);
            }
        }
        return implode('&',$parts);
    }

    /**
     * Stable JSON signature of a metric map (minus synthetic labels)
     */
    private function signature(array $met): string
    {
        unset($met['chrono_timeframe'], $met['_command']);
        ksort($met);
        return json_encode($met);
    }

    /**
     * Deduplicate series: group by signature, then flatten all chrono_timeframe slices.
     */
    private function dedupeSeries(array $allSeries): array
    {
        $bySig = [];
        foreach ($allSeries as $s) {
            $sig = $this->signature($s['metric']);
            $bySig[$sig][] = $s;
        }
        $out = [];
        foreach ($bySig as $grp) {
            foreach ($grp as $series) {
                $out[] = $series;
            }
        }
        return $out;
    }

    /**
     * Build per-signature lastMonthAverage series.
     * Averages across 7/14/21/28-day slices, one line per metric signature.
     */
    private function buildLastMonthAverage(array $seriesList, bool $isRange): array
    {
        $numHist = count($this->timeframes) - 1; // skip 'current'
        if ($numHist < 1) {
            return [];
        }

        $groups = [];
        // group non-current slices by their other labels
        foreach ($seriesList as $s) {
            $tf = $s['metric']['chrono_timeframe'] ?? '';
            if ($tf === 'current') {
                continue;
            }
            $m = $s['metric'];
            unset($m['chrono_timeframe'], $m['_command']);
            ksort($m);
            $sig = json_encode($m);
            $groups[$sig][] = $s;
        }

        $out = [];
        foreach ($groups as $sig => $grp) {
            $sums = [];
            // bucket all points into minute slots
            foreach ($grp as $series) {
                $pts = $isRange ? $series['values'] : [$series['value']];
                foreach ($pts as [$ts, $val]) {
                    $min = intdiv((int)$ts, 60) * 60;
                    $sums[$min] = ($sums[$min] ?? 0) + (float)$val;
                }
            }
            ksort($sums);
            $ptsOut = [];
            foreach ($sums as $min => $sum) {
                $ptsOut[] = [$min, (string)($sum / $numHist)];
            }
            $metric = json_decode($sig, true);
            $metric['chrono_timeframe'] = 'lastMonthAverage';
            $out[] = [
                'metric' => $metric,
                $isRange ? 'values' : 'value'
                    => $isRange ? $ptsOut : end($ptsOut),
            ];
        }

        return $out;
    }

    /**
     * Parse a Prometheus time (RFC3339 or integer) into epoch seconds.
     */
    private function parseTime($t): int
    {
        if (is_numeric($t)) {
            return (int)$t;
        }
        $ts = strtotime((string)$t);
        return $ts !== false ? $ts : time();
    }
}
