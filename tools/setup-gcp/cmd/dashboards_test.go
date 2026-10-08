// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

const (
	dashboardDir = "../dashboards"
	registryPath = "../../../docs/metrics/registry/metrics.yaml"
)

// mustDivideBy names, for each histogram, the attribute that a quantile must
// constrain or group by. The registry note of each metric gives the reason: a
// quantile across the values of this attribute mixes distributions that do not
// compare.
var mustDivideBy = map[string]string{
	"ate.actor.lifecycle.operation.duration": "ate.actor.operation.name",
	"atenet.router.route.duration":           "ate.router.resume",
	"atelet.snapshot.size":                   "file.name",
}

type metricsRegistry struct {
	Groups []struct {
		ID         string `json:"id"`
		Type       string `json:"type"`
		MetricName string `json:"metric_name"`
		Attributes []struct {
			ID   string `json:"id"`
			Ref  string `json:"ref"`
			Type any    `json:"type"`
		} `json:"attributes"`
	} `json:"groups"`
}

// registryMetrics returns the attribute refs of each metric, and the permitted
// values of each enum attribute.
func registryMetrics(t *testing.T) (map[string][]string, map[string][]string) {
	t.Helper()
	raw, err := os.ReadFile(registryPath)
	if err != nil {
		t.Fatalf("read %s: %v", registryPath, err)
	}
	var reg metricsRegistry
	if err := yaml.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("parse %s: %v", registryPath, err)
	}
	metrics := map[string][]string{}
	enums := map[string][]string{}
	for _, g := range reg.Groups {
		switch g.Type {
		case "metric":
			var refs []string
			for _, a := range g.Attributes {
				refs = append(refs, a.Ref)
			}
			metrics[g.MetricName] = refs
		case "attribute_group":
			for _, a := range g.Attributes {
				typ, ok := a.Type.(map[string]any)
				if !ok {
					continue
				}
				members, _ := typ["members"].([]any)
				for _, m := range members {
					if v, ok := m.(map[string]any)["value"].(string); ok {
						enums[a.ID] = append(enums[a.ID], v)
					}
				}
			}
		}
	}
	return metrics, enums
}

var (
	// {"metric_bucket", matchers...}: the UTF-8 selector form that every
	// registry metric needs, because its name has dots.
	selectorRE = regexp.MustCompile(`\{"([^"]+)"([^}]*)\}`)
	// "label"=~"value": only quoted labels are registry attributes. Unquoted
	// labels such as top_level_controller_name come from the target.
	matcherRE = regexp.MustCompile(`"([^"]+)"\s*(=~|!~|!=|=)\s*"([^"]*)"`)
	byRE      = regexp.MustCompile(`by \(([^)]*)\)`)
)

// registryName removes the suffix that the Prometheus export adds to a
// histogram series.
func registryName(series string) string {
	for _, s := range []string{"_bucket", "_count", "_sum"} {
		if n, ok := strings.CutSuffix(series, s); ok {
			return n
		}
	}
	return series
}

func dashboardQueries(t *testing.T, file string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dashboardDir, file))
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	d := &dashboardpb.Dashboard{}
	if err := protojson.Unmarshal(data, d); err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	var queries []string
	for _, tile := range d.GetMosaicLayout().GetTiles() {
		for _, ds := range tile.GetWidget().GetXyChart().GetDataSets() {
			if q := ds.GetTimeSeriesQuery().GetPrometheusQuery(); q != "" {
				queries = append(queries, q)
			}
		}
	}
	return queries
}

// queryErrors returns each way that one PromQL query of a dashboard disagrees
// with the registry.
func queryErrors(q string, metrics, enums map[string][]string) []string {
	var errs []string
	var byLabels []string
	for _, m := range byRE.FindAllStringSubmatch(q, -1) {
		for _, l := range strings.Split(m[1], ",") {
			if l = strings.TrimSpace(l); strings.HasPrefix(l, `"`) {
				byLabels = append(byLabels, strings.Trim(l, `"`))
			}
		}
	}
	for _, sel := range selectorRE.FindAllStringSubmatch(q, -1) {
		name := registryName(sel[1])
		attrs, ok := metrics[name]
		if !ok {
			errs = append(errs, name+" is not a metric in the registry")
			continue
		}
		divided := false
		for _, l := range byLabels {
			if !slices.Contains(attrs, l) {
				errs = append(errs, "group by "+l+" is not an attribute of "+name)
			}
			divided = divided || l == mustDivideBy[name]
		}
		for _, m := range matcherRE.FindAllStringSubmatch(sel[2], -1) {
			label, op, value := m[1], m[2], m[3]
			if !slices.Contains(attrs, label) {
				errs = append(errs, label+" is not an attribute of "+name)
				continue
			}
			if label == mustDivideBy[name] && op == "=" {
				divided = true
			}
			members, isEnum := enums[label]
			if !isEnum || (op != "=" && op != "=~") {
				continue
			}
			for _, v := range strings.Split(value, "|") {
				if !slices.Contains(members, v) {
					errs = append(errs, label+"="+v+" is not a value in the registry")
				}
			}
		}
		if key, ok := mustDivideBy[name]; ok && strings.HasPrefix(q, "histogram_quantile") && !divided {
			errs = append(errs, "the quantile of "+name+" does not constrain or group by "+key)
		}
	}
	return errs
}

// TestDashboardsMatchTheRegistry holds the dashboard queries and metrics.yaml
// in step, so a renamed metric, label or value fails here and not on a blank
// panel.
func TestDashboardsMatchTheRegistry(t *testing.T) {
	t.Parallel()
	metrics, enums := registryMetrics(t)
	for _, file := range dashboardFiles {
		for _, q := range dashboardQueries(t, file) {
			for _, err := range queryErrors(q, metrics, enums) {
				t.Errorf("%s: %s\n  query: %s", file, err, q)
			}
		}
	}
}

func TestQueryErrors(t *testing.T) {
	t.Parallel()
	metrics, enums := registryMetrics(t)
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "warm route quantile",
			query: `histogram_quantile(0.99, sum by (le) (rate({"atenet.router.route.duration_bucket", top_level_controller_name="atenet-router", "ate.router.resume"="none"}[5m])))`,
		},
		{
			name:  "quantile grouped by the essential key",
			query: `histogram_quantile(0.99, sum by (le, "file.name") (rate({"atelet.snapshot.size_bucket"}[1h])))`,
		},
		{
			name:  "rate across resume values",
			query: `sum by ("ate.router.outcome") (rate({"atenet.router.route.duration_count"}[5m]))`,
		},
		{
			name:  "quantile across resume values",
			query: `histogram_quantile(0.99, sum by (le) (rate({"atenet.router.route.duration_bucket"}[5m])))`,
			want:  []string{"the quantile of atenet.router.route.duration does not constrain or group by ate.router.resume"},
		},
		{
			name:  "negative matcher does not divide",
			query: `histogram_quantile(0.99, sum by (le) (rate({"atenet.router.route.duration_bucket", "ate.router.resume"!="joined"}[5m])))`,
			want:  []string{"the quantile of atenet.router.route.duration does not constrain or group by ate.router.resume"},
		},
		{
			name:  "regex matcher without group by does not divide",
			query: `histogram_quantile(0.99, sum by (le) (rate({"atelet.snapshot.size_bucket", "file.name"=~"pages.img|memory-ranges"}[1h])))`,
			want:  []string{"the quantile of atelet.snapshot.size does not constrain or group by file.name"},
		},
		{
			name:  "unknown metric",
			query: `sum(rate({"atenet.router.route.latency_count"}[5m]))`,
			want:  []string{"atenet.router.route.latency is not a metric in the registry"},
		},
		{
			name:  "unknown attribute",
			query: `sum by ("ate.template.id") (rate({"atelet.snapshot.size_count", "file.kind"="pages"}[1h]))`,
			want: []string{
				"group by ate.template.id is not an attribute of atelet.snapshot.size",
				"file.kind is not an attribute of atelet.snapshot.size",
			},
		},
		{
			name:  "unknown enum value",
			query: `sum(rate({"atenet.router.route.duration_count", "ate.router.outcome"=~"ok|success"}[5m]))`,
			want:  []string{"ate.router.outcome=success is not a value in the registry"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := queryErrors(tt.query, metrics, enums); !slices.Equal(got, tt.want) {
				t.Errorf("queryErrors() = %q, want %q", got, tt.want)
			}
		})
	}
}
