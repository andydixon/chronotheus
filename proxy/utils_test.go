// proxy/utils_test.go
package proxy

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ─── parseTime ─────────────────────────────────────────────────────────────────

func TestParseTime(t *testing.T) {
	now := time.Now().Unix()
	cases := []struct {
		in   string
		want int64
	}{
		{"1600000000", 1600000000},
		{"2020-09-13T12:00:00Z", 1600001600},
		{"", now},
		{"bogus", now},
	}
	for _, tc := range cases {
		got := parseTime(tc.in)
		if tc.in == "" || tc.in == "bogus" {
			// allow ±2s
			if diff := got - now; diff < -2 || diff > 2 {
				t.Errorf("parseTime(%q) = %d; want ≈%d", tc.in, got, now)
			}
		} else if got != tc.want {
			t.Errorf("parseTime(%q) = %d; want %d", tc.in, got, tc.want)
		}
	}
}

// ─── buildQueryString ──────────────────────────────────────────────────────────

func TestBuildQueryString_Simple(t *testing.T) {
	v := url.Values{}
	v.Set("foo", "bar")
	qs := buildQueryString(v)
	if qs != "foo=bar" {
		t.Errorf("got %q, want foo=bar", qs)
	}
}

func TestBuildQueryString_Array(t *testing.T) {
	v := url.Values{}
	v.Add("match", `a="1"`)
	v.Add("match", `b="2"`)
	qs := buildQueryString(v)
	parsed, err := url.ParseQuery(qs)
	if err != nil {
		t.Fatalf("ParseQuery(%q): %v", qs, err)
	}
	want := url.Values{"match": {`a="1"`, `b="2"`}}
	if !reflect.DeepEqual(parsed, want) {
		t.Errorf("round-trip: got %v; want %v", parsed, want)
	}
}

// ─── stripLabelFromParam ───────────────────────────────────────────────────────

func TestStripLabelFromParam(t *testing.T) {
	cases := []struct{ in, label, want string }{
		{`{a="1",chrono_timeframe="7days",b="2"}`, "chrono_timeframe", `{a="1",b="2"}`},
		{`{,chrono_timeframe="7days",a="1"}`, "chrono_timeframe", `{a="1"}`},
		{`{a="1",b="2",chrono_timeframe="7days",}`, "chrono_timeframe", `{a="1",b="2"}`},
	}
	for _, tc := range cases {
		vals := url.Values{"query": {tc.in}}
		stripLabelFromParam(vals, "query", tc.label)
		if got := vals.Get("query"); got != tc.want {
			t.Errorf("stripLabel(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// ─── remapMatch ────────────────────────────────────────────────────────────────

func TestRemapMatch(t *testing.T) {
	vals := url.Values{"match": {`a="1"`, `b="2"`}}
	remapMatch(vals)
	if vals.Get("match") != "" {
		t.Errorf("expected original match removed, got %v", vals["match"])
	}
	if !reflect.DeepEqual(vals["match[]"], []string{`a="1"`, `b="2"`}) {
		t.Errorf("got match[]=%v", vals["match[]"])
	}
}

// ─── detectSelectors ───────────────────────────────────────────────────────────

func TestDetectSelectors(t *testing.T) {
	v := url.Values{}
	v.Set("query", `{foo="bar",chrono_timeframe="14days",_command="dryRun"}`)
	tf, cmd := detectSelectors(v)
	if tf != "14days" || cmd != "dryRun" {
		t.Errorf("got (%q,%q); want (14days,dryRun)", tf, cmd)
	}
}

// ─── signature ─────────────────────────────────────────────────────────────────

func TestSignature_IgnoresSyntheticAndSorts(t *testing.T) {
	m := map[string]interface{}{
		"b":                "two",
		"a":                "one",
		"chrono_timeframe": "7days",
		"_command":         "x",
	}
	sig := signature(m)
	want := `{"a":"one","b":"two"}`
	if sig != want {
		t.Errorf("signature = %q; want %q", sig, want)
	}
}

// ─── dedupeSeries ──────────────────────────────────────────────────────────────

func TestDedupeSeries(t *testing.T) {
	s1 := map[string]interface{}{"metric": map[string]interface{}{"a": "1"}}
	s2 := map[string]interface{}{"metric": map[string]interface{}{"a": "1"}}
	s3 := map[string]interface{}{"metric": map[string]interface{}{"a": "2"}}
	in := []map[string]interface{}{s1, s2, s3}
	out := dedupeSeries(in)
	if len(out) != 3 {
		t.Errorf("len=%d; want 3", len(out))
	}
}

// ─── buildLastMonthAverage (vector) ────────────────────────────────────────────

func TestBuildLastMonthAverage_Vector(t *testing.T) {
	// four historical slices, same signature, single point
	tfs := proxyTimeframes()[1:] // skip "current"
	var input []map[string]interface{}
	for i, tf := range tfs {
		val := fmt.Sprintf("%d", (i+1)*10)
		input = append(input, map[string]interface{}{
			"metric": map[string]interface{}{"a": "1", "chrono_timeframe": tf},
			"value":  []interface{}{100, val},
		})
	}
	arr := buildLastMonthAverage(input, false)
	if len(arr) != 1 {
		t.Fatalf("got %d series; want 1", len(arr))
	}
	pt := arr[0]["value"].([]interface{})
	if pt[0].(int64) != 100 {
		t.Errorf("timestamp=%v; want 100", pt[0])
	}
	// average of 10+20+30+40 = 25
	if pt[1].(string) != "25" {
		t.Errorf("value=%v; want 25", pt[1])
	}
}

// ─── containsString ────────────────────────────────────────────────────────────

func TestContainsString(t *testing.T) {
	arr := []interface{}{"foo", "bar"}
	if !containsString(arr, "bar") {
		t.Errorf("should contain 'bar'")
	}
	if containsString(arr, "baz") {
		t.Errorf("should not contain 'baz'")
	}
}

// ─── filterByTimeframe ──────────────────────────────────────────────────────────

func TestFilterByTimeframe(t *testing.T) {
	data := []map[string]interface{}{
		{"metric": map[string]interface{}{"chrono_timeframe": "current"}, "v": 1},
		{"metric": map[string]interface{}{"chrono_timeframe": "7days"}, "v": 2},
	}
	out := filterByTimeframe(data, "7days")
	if len(out) != 1 {
		t.Errorf("got %d; want 1", len(out))
	}
	m := out[0]["metric"].(map[string]interface{})
	if m["chrono_timeframe"] != "7days" {
		t.Errorf("got %v", m["chrono_timeframe"])
	}
}

// ─── indexBySignature ──────────────────────────────────────────────────────────

func TestIndexBySignature(t *testing.T) {
	all := []map[string]interface{}{
		{"metric": map[string]interface{}{"a": "1", "chrono_timeframe": "current"}, "v": 1},
		{"metric": map[string]interface{}{"a": "1", "chrono_timeframe": "7days"}, "v": 2},
	}
	avg := []map[string]interface{}{
		{"metric": map[string]interface{}{"a": "1", "chrono_timeframe": "lastMonthAverage"}, "v": 3},
	}
	cur, a := indexBySignature(all, avg)
	if len(cur) != 1 || len(a) != 1 {
		t.Errorf("cur=%d,avg=%d; want 1,1", len(cur), len(a))
	}
}

// ─── Test parseClientParams (GET, JSON and Form) ───────────────────────────────

func TestParseClientParams(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		queryParams    map[string]string
		postBody       string
		contentType    string
		expectedParams map[string][]string
	}{
		{
			name:   "GET request with simple params",
			method: "GET",
			queryParams: map[string]string{
				"query": "test_metric",
				"time":  "1622505600",
			},
			expectedParams: map[string][]string{
				"query": {"test_metric"},
				"time":  {"1622505600"},
			},
		},
		{
			name:        "POST request with JSON body",
			method:      "POST",
			contentType: "application/json",
			postBody:    `{"query":"test_metric","time":"1622505600"}`,
			expectedParams: map[string][]string{
				"query": {"test_metric"},
				"time":  {"1622505600"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test request
			req := httptest.NewRequest(tt.method, "http://localhost:8080", strings.NewReader(tt.postBody))

			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			// Add query parameters
			q := req.URL.Query()
			for k, v := range tt.queryParams {
				q.Add(k, v)
			}
			req.URL.RawQuery = q.Encode()

			// Parse params
			params := parseClientParams(req)

			// Verify results
			for k, expectedVals := range tt.expectedParams {
				actualVals, ok := params[k]
				if !ok {
					t.Errorf("Expected param %s not found", k)
					continue
				}
				if !reflect.DeepEqual(actualVals, expectedVals) {
					t.Errorf("For param %s, expected %v, got %v", k, expectedVals, actualVals)
				}
			}
		})
	}
}

func TestAppendCompare(t *testing.T) {
	tests := []struct {
		name      string
		current   float64
		average   float64
		expected  float64
		isRange   bool
	}{
		{
			name:     "Simple difference",
			current:  150,
			average:  100,
			expected: 50,
			isRange:  false,
		},
		{
			name:     "Negative difference",
			current:  80,
			average:  100,
			expected: -20,
			isRange:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curMap := map[string]map[string]interface{}{
				"test": {
					"value": []interface{}{float64(100), fmt.Sprintf("%v", tt.current)},
				},
			}
			avgMap := map[string]map[string]interface{}{
				"test": {
					"value": []interface{}{float64(100), fmt.Sprintf("%v", tt.average)},
				},
			}

			result := appendCompare(nil, curMap, avgMap, "", tt.isRange)

			if len(result) != 1 {
				t.Fatalf("Expected 1 result, got %d", len(result))
			}

			val := result[0]["value"].([]interface{})
			resultVal, _ := strconv.ParseFloat(val[1].(string), 64)
			if resultVal != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, resultVal)
			}
		})
	}
}

func TestAppendPercent(t *testing.T) {
	tests := []struct {
		name      string
		current   float64
		average   float64
		expected  float64
		isRange   bool
	}{
		{
			name:     "50% increase",
			current:  150,
			average:  100,
			expected: 50,
			isRange:  false,
		},
		{
			name:     "20% decrease",
			current:  80,
			average:  100,
			expected: -20,
			isRange:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curMap := map[string]map[string]interface{}{
				"test": {
					"value": []interface{}{float64(100), fmt.Sprintf("%v", tt.current)},
				},
			}
			avgMap := map[string]map[string]interface{}{
				"test": {
					"value": []interface{}{float64(100), fmt.Sprintf("%v", tt.average)},
				},
			}

			result := appendPercent(nil, curMap, avgMap, "", tt.isRange)

			if len(result) != 1 {
				t.Fatalf("Expected 1 result, got %d", len(result))
			}

			val := result[0]["value"].([]interface{})
			resultVal, _ := strconv.ParseFloat(val[1].(string), 64)
			if resultVal != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, resultVal)
			}
		})
	}
}
