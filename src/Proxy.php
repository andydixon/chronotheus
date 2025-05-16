<?php

namespace Andydixon\Chronotheus;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;

/**
 * Class Proxy
 * 
 * This class acts as a proxy for forwarding and transforming API requests to an upstream server.
 * It supports Prometheus-style queries and labels, with additional functionality for handling
 * historical data and synthetic labels. The proxy adjusts query parameters, forwards requests,
 * and processes responses to include custom labels or aggregated data.
 * 
 * Key Features:
 * - Handles Prometheus instant and range queries.
 * - Supports synthetic labels like `chrono_timeframe` and `lastMonthAverage`.
 * - Adjusts timestamps and deduplicates time-series data.
 * - Provides fallback proxying for unsupported paths.
 * 
 * Dependencies:
 * - GuzzleHttp\Client for making HTTP requests.
 * - PHP's built-in functions for parsing and manipulating data.
 */
class Proxy
{
    /** Offsets in seconds: 0d, 7d, 14d, 21d, 28d */
    private array $offsets = [
        0,
        7 * 24 * 3600,
        14 * 24 * 3600,
        21 * 24 * 3600,
        28 * 24 * 3600,
    ];

    /** Corresponding chrono_timeframe labels */
    private array $historicals = [
        "current",
        "7days",
        "14days",
        "21days",
        "28days",
    ];

    /**
     * Entrypoint: parse /<host>_<port>/ prefix, then dispatch.
     * 
     * @param string $uri The request URI.
     * @param array $server Server variables, typically $_SERVER.
     * @param string|null $body The raw request body, if any.
     * 
     * @return void
     */
    public function handle(
        string $uri,
        array $server,
        ?string $body = null
    ): void {
        $path = parse_url($uri, PHP_URL_PATH) ?: "/";
        if (!preg_match('#^/([^_/]+)_(\d+)(/.*)?$#', $path, $m)) {
            http_response_code(400);
            header("Content-Type: application/json");
            echo json_encode([
                "status" => "error",
                "error" => "Invalid target prefix",
            ]);
            return;
        }
        [, $host, $port, $suffix] = $m;
        $suffix = $suffix ?: "/";
        $upstream = "http://{$host}:{$port}";
        $method = strtoupper($server["REQUEST_METHOD"]);

        // route to the correct handler
        if (
            preg_match('#^/api/v1/query$#', $suffix) &&
            in_array($method, ["GET", "POST"], true)
        ) {
            $this->handleQuery($upstream, "/api/v1/query", $server, $body);
        } elseif (
            preg_match('#^/api/v1/query_range$#', $suffix) &&
            in_array($method, ["GET", "POST"], true)
        ) {
            $this->handleQueryRange(
                $upstream,
                "/api/v1/query_range",
                $server,
                $body
            );
        } elseif (
            in_array($method, ["GET", "POST"], true) &&
            preg_match('#^/api/v1/labels/?$#', $suffix)
        ) {
            $this->handleLabels($upstream, "/api/v1/labels");
        } elseif (
            in_array($method, ["GET", "POST"], true) &&
            preg_match('#^/api/v1/label/([^/]+)/values/?$#', $suffix, $nm)
        ) {
            $this->handleLabelValues(
                $upstream,
                "/api/v1/label/{$nm[1]}/values",
                $nm[1]
            );
        } else {
            $this->forward($upstream . $suffix, $method, $body);
        }
    }

    /**
     * Instant query: /api/v1/query
     * 
     * Handles Prometheus instant queries by forwarding the request to the upstream server.
     * Adjusts timestamps for historical data and aggregates results.
     * 
     * @param string $upstream The upstream server URL.
     * @param string $path The API path to query.
     * @param array $server Server variables, typically $_SERVER.
     * @param string|null $body The raw request body, if any.
     * 
     * @return void
     */
    private function handleQuery(
        string $upstream,
        string $path,
        array $server,
        ?string $body
    ): void {
        $method = strtoupper($server["REQUEST_METHOD"]);
        $contentType = $server["CONTENT_TYPE"] ?? "";
        $requestedHistorical = null;


        // parse GET or JSON/form-POST params
        if ($method === "POST") {
            if (stripos($contentType, "application/json") !== false) {
                $params = json_decode($body ?? "", true) ?: [];
            } else {
                parse_str($body ?? "", $params);
            }
        } else {
            $params = $_GET;
        }

        if (
            isset($params['query'])
            && preg_match('/chrono_timeframe="([^"]+)"/', $params['query'], $m)
        ) {
            $requestedHistorical = $m[1]; // e.g. "7days" or "lastMonthAverage"
        }

        // strip any user-supplied chrono_timeframe matcher
        $this->stripHistoricalFromParam($params, "query");

        $client = new Client();
        $allSeries = [];

        // fetch 5 windows in parallel (0d, 7d, 14d, 21d, 28d)
        foreach ($this->offsets as $i => $offset) {
            $histLabel = $this->historicals[$i];

            // adjust the 'time' param backward by $offset
            $timeEpoch = $this->parseTime($params["time"] ?? null);
            $params["time"] = $timeEpoch - $offset;

            // upstream request
            try {
                $resp = $client->request("GET", $upstream . $path, [
                    "query" => $params,
                ]);
            } catch (GuzzleException $e) {
                error_log(
                    sprintf(
                        "[Chrono][Upstream ERROR] %s %s?%s → %s",
                        $method,
                        $upstream . $path,
                        http_build_query($params),
                        $e->getMessage()
                    )
                );
                http_response_code(502);
                header("Content-Type: application/json");
                echo json_encode([
                    "status" => "error",
                    "error" => "Upstream request failed",
                ]);
                return;
            }

            $data = json_decode($resp->getBody()->getContents(), true);

            // shift timestamps forward & tag, preserving string values
            foreach ($data["data"]["result"] as $series) {
                [$ts, $val] = $series["value"];
                $series["value"] = [$ts + $offset, (string) $val];
                $series["metric"]["chrono_timeframe"] = $histLabel;
                $allSeries[] = $series;
            }
        }

        // dedupe and build the lastMonthAverage series
        $merged = $this->dedupeSeries($allSeries);
        $avgSeriesA = $this->buildLastMonthAverage($merged, false);
        // append each per-signature average-series
        foreach ($avgSeriesA as $avgSeries) {
            $merged[] = $avgSeries;
        }

        // if they asked for a single chrono_timeframe slice, filter down to only that
        if ($requestedHistorical !== null) {
            $filtered = array_values(array_filter(
                $merged,
                fn($s) => ($s['metric']['chrono_timeframe'] ?? '') === $requestedHistorical
            ));

            header('Content-Type: application/json');
            echo json_encode([
                'status' => 'success',
                'data' => [
                    'resultType' => 'vector',
                    'result'     => $filtered,
                ],
            ]);
            return;
        }




        header("Content-Type: application/json");
        echo json_encode([
            "status" => "success",
            "data" => [
                "resultType" => "vector",
                "result" => array_values($merged),
            ],
        ]);
    }

    /**
     * Range query: /api/v1/query_range
     * 
     * Handles Prometheus range queries by forwarding the request to the upstream server.
     * Adjusts timestamps for historical data and aggregates results.
     * 
     * @param string $upstream The upstream server URL.
     * @param string $path The API path to query.
     * @param array $server Server variables, typically $_SERVER.
     * @param string|null $body The raw request body, if any.
     * 
     * @return void
     */
    private function handleQueryRange(
        string $upstream,
        string $path,
        array $server,
        ?string $body
    ): void {
        $method = strtoupper($server["REQUEST_METHOD"]);
        $contentType = $server["CONTENT_TYPE"] ?? "";

        // parse GET or JSON/form-POST params
        if ($method === "POST") {
            if (stripos($contentType, "application/json") !== false) {
                $params = json_decode($body ?? "", true) ?: [];
            } else {
                parse_str($body ?? "", $params);
            }
        } else {
            $params = $_GET;
        }

        // detect if the user asked for a specific historical slice
        $requestedHistorical = null;
        if (
            isset($params['query'])
            && preg_match('/chrono_timeframe="([^"]+)"/', $params['query'], $m)
        ) {
            $requestedHistorical = $m[1]; // e.g. "7days" or "lastMonthAverage"
        }


        // strip any user-supplied chrono_timeframe matcher
        $this->stripHistoricalFromParam($params, "query");

        // default step=60 if missing
        if (empty($params["step"])) {
            $params["step"] = 60;
        }

        $client = new Client();
        $allSeries = [];

        // fetch 5 windows
        foreach ($this->offsets as $i => $offset) {
            $histLabel = $this->historicals[$i];
            if (@$_GET['ajdebug']) print_r("\n\n\n" . $histLabel . " offset: $offset" . " $i\n");
            // shift start/end backwards
            $params["start"] =
                $this->parseTime($params["start"] ?? null) - $offset;
            $params["end"] = $this->parseTime($params["end"] ?? null) - $offset;


            try {
                $resp = $client->request("GET", $upstream . $path, [
                    "query" => $params,
                ]);
            } catch (GuzzleException $e) {
                error_log(
                    sprintf(
                        "[Chrono][Upstream ERROR] %s %s?%s → %s",
                        $method,
                        $upstream . $path,
                        http_build_query($params),
                        $e->getMessage()
                    )
                );
                http_response_code(502);
                header("Content-Type: application/json");
                echo json_encode([
                    "status" => "error",
                    "error" => "Upstream request failed",
                ]);

                return;
            }

            $data = json_decode($resp->getBody()->getContents(), true);

            // shift + tag, preserving string values
            foreach ($data["data"]["result"] as $series) {
                $shifted = [];
                foreach ($series["values"] as [$ts, $val]) {
                    $shifted[] = [$ts + $offset, (string) $val];
                }
                $series["values"] = $shifted;
                $series["metric"]["chrono_timeframe"] = $histLabel;
                $allSeries[] = $series;
                if ($histLabel !== "current") {
                    if (@$_GET['debug']) print_r($series);
                }
            }
        }

        // dedupe and build average
        $merged = $this->dedupeSeries($allSeries);
        $avgSeriesA  = $this->buildLastMonthAverage($merged, true);
        foreach ($avgSeriesA as $avgSeries) {
            $merged[] = $avgSeries;
        }



        // if they asked for a single chrono_timeframe slice, filter down to only that
        if ($requestedHistorical !== null) {
            $filtered = array_values(array_filter(
                $merged,
                fn($s) => ($s['metric']['chrono_timeframe'] ?? '') === $requestedHistorical
            ));

            header('Content-Type: application/json');
            echo json_encode([
                'status' => 'success',
                'data' => [
                    'resultType' => 'matrix',
                    'result'     => $filtered,
                ],
            ]);
            return;
        }

        header("Content-Type: application/json");
        echo json_encode([
            "status" => "success",
            "data" => [
                "resultType" => "matrix",
                "result" => array_values($merged),
            ],
        ]);
    }

    /**
     * GET/POST /api/v1/labels — forward client params, then append chrono_timeframe
     * 
     * Processes label queries by forwarding the request to the upstream server.
     * Ensures the `chrono_timeframe` label is included in the response.
     * 
     * @param string $upstream The upstream server URL.
     * @param string $path The API path to query.
     * 
     * @return void
     */
    private function handleLabels(string $upstream, string $path): void
    {
        $params = $this->parseClientParams();
        $this->stripHistoricalFromMatches($params);

        // Remap PHP’s "match" → "match[]" if needed
        if (isset($params["match"]) && !isset($params["match[]"])) {
            $params["match[]"] = $params["match"];
            unset($params["match"]);
        }

        // Build the exact query string
        $qs = $this->buildQueryString($params);
        $url = "{$upstream}{$path}?{$qs}";

        try {
            $resp = (new Client())->request("GET", $url);
        } catch (GuzzleException $e) {
            error_log(
                "[Chrono][Upstream ERROR] handleLabels → {$e->getMessage()}"
            );
            http_response_code(502);
            header("Content-Type: application/json");
            echo json_encode([
                "status" => "error",
                "error" => "Upstream request failed",
            ]);
            return;
        }

        $data = json_decode($resp->getBody()->getContents(), true);

        // Ensure our pseudo-label is present
        if (!isset($data["data"]) || !is_array($data["data"])) {
            $data["status"] = "success";
            $data["data"] = ["chrono_timeframe"];
        } elseif (!in_array("chrono_timeframe", $data["data"], true)) {
            $data["data"][] = "chrono_timeframe";
        }

        header("Content-Type: application/json");
        echo json_encode($data);
    }

    /**
     * GET/POST /api/v1/label/{name}/values — forward client params minus chrono_timeframe
     * 
     * Processes label value queries by forwarding the request to the upstream server.
     * Handles synthetic labels like `chrono_timeframe` and `lastMonthAverage`.
     * 
     * @param string $upstream The upstream server URL.
     * @param string $path The API path to query.
     * @param string $labelName The name of the label being queried.
     * 
     * @return void
     */
    private function handleLabelValues(
        string $upstream,
        string $path,
        string $labelName
    ): void {
        // Synthetic label
        if ($labelName === "chrono_timeframe") {
            header("Content-Type: application/json");
            echo json_encode([
                "status" => "success",
                "data" => array_merge($this->historicals, ["lastMonthAverage"]),
            ]);
            return;
        }

        $params = $this->parseClientParams();
        $this->stripHistoricalFromMatches($params);

        // Remap PHP’s "match" → "match[]" if needed
        if (isset($params["match"]) && !isset($params["match[]"])) {
            $params["match[]"] = $params["match"];
            unset($params["match"]);
        }

        $qs = $this->buildQueryString($params);
        $url = "{$upstream}{$path}?{$qs}";

        try {
            $resp = (new Client())->request("GET", $url);
        } catch (GuzzleException $e) {
            error_log(
                "[Chrono][Upstream ERROR] handleLabelValues → {$e->getMessage()}"
            );
            http_response_code(502);
            header("Content-Type: application/json");
            echo json_encode([
                "status" => "error",
                "error" => "Upstream request failed",
            ]);
            return;
        }

        $data = json_decode($resp->getBody()->getContents(), true);

        header("Content-Type: application/json");
        echo json_encode($data);
    }

    /**
     * Fallback proxy for all other paths.
     * 
     * Forwards unsupported paths to the upstream server without modification.
     * 
     * @param string $url The full URL to forward the request to.
     * @param string $method The HTTP method (GET, POST, etc.).
     * @param string|null $body The raw request body, if any.
     * 
     * @return void
     */
    private function forward(string $url, string $method, ?string $body): void
    {
        $client = new Client();
        $opts = $method === "GET" ? ["query" => $_GET] : ["body" => $body];

        $resp = $client->request($method, $url, $opts);

        header("Content-Type: {$resp->getHeaderLine("Content-Type")}");
        echo $resp->getBody()->getContents();
    }

    /**
     * Strip any chrono_timeframe="..." matcher from a PromQL param.
     * 
     * @param array $params The query parameters to modify.
     * @param string $key The key of the parameter to strip.
     * 
     * @return void
     */
    private function stripHistoricalFromParam(array &$params, string $key): void
    {
        if (!isset($params[$key])) {
            return;
        }
        $params[$key] = preg_replace(
            '/[, ]+?chrono_timeframe="[^"]*"/',
            "",
            $params[$key]
        );
    }

    /**
     * Deduplicate time-series by metric signature (ignoring chrono_timeframe).
     * 
     * @param array $allSeries The list of time-series data to deduplicate.
     * 
     * @return array The deduplicated time-series data.
     */
    private function dedupeSeries(array $allSeries): array
    {
        return $allSeries;
        $buckets = [];
        foreach ($allSeries as $s) {
            $m = $s["metric"];
            unset($m["chrono_timeframe"]);
            ksort($m);
            $sig = json_encode($m);
            $buckets[$sig][] = $s;
        }
        $out = [];
        foreach ($buckets as $group) {
            foreach ($group as $series) {
                $out[] = $series;
            }
        }
        return $out;
    }

    /**
     * Build lastMonthAverage per signature, with global fallback.
     *
     * @param array $seriesList  All merged series including chrono_timeframe tag
     * @param bool  $isRange     True if matrix (query_range), false if vector (query)
     * @return array             List of average-series objects (never empty if hist data exists)
     */
    private function buildLastMonthAverage(array $seriesList, bool $isRange): array
    {
        $numHist = count($this->historicals) - 1; // always 4 in 7/14/21/28
        if ($numHist < 1) {
            return [];
        }

        // 1) Group by signature (excluding 'current')
        $groups = [];
        foreach ($seriesList as $s) {
            $label = $s['metric']['chrono_timeframe'] ?? null;
            if ($label === 'current') {
                continue;
            }
            $m = $s['metric'];
            unset($m['chrono_timeframe']);
            ksort($m);
            $sig = json_encode($m);
            $groups[$sig][] = $s;
        }

        $out = [];

        // 2) If we found per-signature groups, average each
        if (!empty($groups)) {
            foreach ($groups as $sig => $groupSeries) {
                $sums = [];
                foreach ($groupSeries as $series) {
                    $points = $isRange ? $series['values'] : [$series['value']];
                    foreach ($points as [$ts, $val]) {
                        $minute = intdiv((int)$ts, 60) * 60;
                        $sums[$minute] = ($sums[$minute] ?? 0) + (float)$val;
                    }
                }
                ksort($sums);
                $pts = [];
                foreach ($sums as $minute => $sum) {
                    $pts[] = [$minute, (string)($sum / $numHist)];
                }
                $metric         = json_decode($sig, true);
                $metric['chrono_timeframe'] = 'lastMonthAverage';
                $out[] = [
                    'metric' => $metric,
                    $isRange ? 'values' : 'value' => $isRange ? $pts : end($pts),
                ];
            }
        }

        // 3) Global fallback: if no groups, average everything together
        if (empty($out) && !empty($seriesList)) {
            $sums = [];
            foreach ($seriesList as $s) {
                if (($s['metric']['chrono_timeframe'] ?? '') === 'current') {
                    continue;
                }
                $points = $isRange ? $s['values'] : [$s['value']];
                foreach ($points as [$ts, $val]) {
                    $minute = intdiv((int)$ts, 60) * 60;
                    $sums[$minute] = ($sums[$minute] ?? 0) + (float)$val;
                }
            }
            ksort($sums);
            $pts = [];
            foreach ($sums as $minute => $sum) {
                // divide by the count of actual historical series we saw:
                $pts[] = [$minute, (string)($sum / max(1, count($seriesList) - 1))];
            }
            $firstMetric = $seriesList[0]['metric'] ?? [];
            unset($firstMetric['chrono_timeframe']);
            ksort($firstMetric);
            $firstMetric['chrono_timeframe'] = 'lastMonthAverage';
            $out[] = [
                'metric' => $firstMetric,
                $isRange ? 'values' : 'value' => $isRange ? $pts : end($pts),
            ];
        }

        return $out;
    }

    /**
     * Parse RFC3339 string or integer into epoch seconds.
     * 
     * @param mixed $t The time value to parse (string or integer).
     * 
     * @return int The parsed epoch seconds.
     */
    private function parseTime($t): int
    {
        if (is_numeric($t)) {
            return (int) $t;
        }
        $ts = strtotime((string) $t);
        return $ts !== false ? $ts : time();
    }

    /**
     * Parse client-supplied parameters:
     * - GET → $_GET
     * - POST JSON → decoded JSON body
     * - POST form → parse_str on raw body
     * 
     * @return array The parsed client parameters.
     */
    private function parseClientParams(): array
    {
        $method = $_SERVER["REQUEST_METHOD"] ?? "GET";
        $contentType = $_SERVER["CONTENT_TYPE"] ?? "";
        $rawBody = file_get_contents("php://input");

        if ($method === "POST") {
            if (stripos($contentType, "application/json") !== false) {
                $decoded = json_decode($rawBody, true);
                return is_array($decoded) ? $decoded : [];
            } else {
                parse_str($rawBody, $params);
                return $params;
            }
        }

        // GET (and others)
        return $_GET;
    }

    /**
     * Remove any chrono_timeframe="..." from all match[] filters in $params.
     * 
     * @param array $params The query parameters to modify.
     * 
     * @return void
     */
    private function stripHistoricalFromMatches(array &$params): void
    {
        // Grafana (and some clients) send match[] as either 'match' key (array) or 'match[]'
        $keys = ["match", "match[]"];
        foreach ($keys as $k) {
            if (!isset($params[$k])) {
                continue;
            }
            if (is_array($params[$k])) {
                $params[$k] = array_map(
                    fn($m) => preg_replace('/[, ]+?chrono_timeframe="[^"]*"/', "", $m),
                    $params[$k]
                );
            } else {
                $params[$k] = preg_replace(
                    '/[, ]+?chrono_timeframe="[^"]*"/',
                    "",
                    $params[$k]
                );
            }
            $params[$k] = preg_replace(
                '/, , /',
                ", ",
                $params[$k]
            );
        }
    }

    /**
     * Build a URL-encoded query string from $params,
     * repeating keys for array values (e.g. match[]=a&match[]=b).
     * 
     * @param array $params The query parameters to encode.
     * 
     * @return string The URL-encoded query string.
     */
    private function buildQueryString(array $params): string
    {
        $parts = [];
        foreach ($params as $key => $value) {
            if (is_array($value)) {
                // ensure the param name ends with []
                $paramName = str_ends_with($key, "[]") ? $key : $key . "[]";
                foreach ($value as $v) {
                    $parts[] =
                        rawurlencode($paramName) . "=" . rawurlencode($v);
                }
            } else {
                $parts[] = rawurlencode($key) . "=" . rawurlencode($value);
            }
        }
        return implode("&", $parts);
    }
}
