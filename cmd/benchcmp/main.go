package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type benchmarkDocument struct {
	Sections  []documentSection `json:"sections"`
	Workload  workloadConfig    `json:"workload"`
	Summaries []summaryResult   `json:"summaries"`
}

type documentSection struct {
	Label  string          `json:"label"`
	Report benchmarkReport `json:"report"`
}

type benchmarkReport struct {
	Workload  workloadConfig  `json:"workload"`
	Summaries []summaryResult `json:"summaries"`
}

type workloadConfig struct {
	Label        string `json:"label"`
	PayloadBytes int    `json:"payloadBytes"`
	Consumers    int    `json:"consumers"`
}

type summaryResult struct {
	Target       string       `json:"target"`
	MedianResult medianResult `json:"medianResult"`
}

type medianResult struct {
	Target         string  `json:"target"`
	PublishRate    float64 `json:"publishRate"`
	ConsumeRate    float64 `json:"consumeRate"`
	LatencyP99Ms   float64 `json:"latencyP99Ms"`
	FIFOViolations int     `json:"fifoViolations"`
	Consumers      int     `json:"consumers"`
	PayloadBytes   int     `json:"payloadBytes"`
}

type compareOptions struct {
	Strict       bool
	BaselineName string
	CurrentName  string
}

type comparisonResult struct {
	Strict       bool
	BaselineName string
	CurrentName  string
	Rows         []comparisonRow
	Gates        []gateResult
}

type comparisonRow struct {
	Section          string
	Target           string
	Baseline         medianResult
	Current          medianResult
	PublishDeltaPct  float64
	ConsumeDeltaPct  float64
	P99DeltaPct      float64
	MissingBaseline  bool
	MissingCurrent   bool
	Status           gateStatus
	GateDescriptions []string
}

type gateResult struct {
	Status  gateStatus
	Gate    string
	Section string
	Target  string
	Detail  string
}

type gateStatus string

const (
	statusOK   gateStatus = "OK"
	statusWarn gateStatus = "WARN"
	statusFail gateStatus = "FAIL"
)

func main() {
	os.Exit(runCompare(os.Args[1:], os.Stdout, os.Stderr))
}

func runCompare(args []string, stdout, stderr io.Writer) int {
	var opts compareOptions
	var baselinePath, currentPath, summaryOut string

	fs := flag.NewFlagSet("benchcmp", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&baselinePath, "baseline", "", "baseline benchmark JSON path")
	fs.StringVar(&currentPath, "current", "", "current benchmark JSON path")
	fs.StringVar(&summaryOut, "summary-out", "", "optional markdown summary output path")
	fs.BoolVar(&opts.Strict, "strict", false, "fail when benchmark gates are violated")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if baselinePath == "" || currentPath == "" {
		fmt.Fprintln(stderr, "-baseline and -current are required")
		return 2
	}

	baseline, err := loadBenchmarkDocument(baselinePath)
	if err != nil {
		fmt.Fprintf(stderr, "load baseline: %v\n", err)
		return 2
	}
	current, err := loadBenchmarkDocument(currentPath)
	if err != nil {
		fmt.Fprintf(stderr, "load current: %v\n", err)
		return 2
	}

	opts.BaselineName = baselinePath
	opts.CurrentName = currentPath
	result := compareBenchmarks(baseline, current, opts)
	summary := renderMarkdown(result)

	if summaryOut != "" {
		if err := os.MkdirAll(filepath.Dir(summaryOut), 0o755); err != nil {
			fmt.Fprintf(stderr, "create summary dir: %v\n", err)
			return 2
		}
		if err := os.WriteFile(summaryOut, []byte(summary), 0o644); err != nil {
			fmt.Fprintf(stderr, "write summary: %v\n", err)
			return 2
		}
	}

	if _, err := io.WriteString(stdout, summary); err != nil {
		fmt.Fprintf(stderr, "write summary to stdout: %v\n", err)
		return 2
	}
	if result.Failed() {
		return 1
	}
	return 0
}

func loadBenchmarkDocument(path string) (benchmarkDocument, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return benchmarkDocument{}, err
	}
	var doc benchmarkDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return benchmarkDocument{}, err
	}
	if len(doc.Sections) == 0 && len(doc.Summaries) == 0 {
		return benchmarkDocument{}, errors.New("no benchmark sections or summaries found")
	}
	return doc, nil
}

func compareBenchmarks(baseline, current benchmarkDocument, opts compareOptions) comparisonResult {
	result := comparisonResult{
		Strict:       opts.Strict,
		BaselineName: opts.BaselineName,
		CurrentName:  opts.CurrentName,
	}

	baselineRows := rowsByKey(rowsFromDocument(baseline))
	currentRows := rowsByKey(rowsFromDocument(current))
	keys := make([]string, 0, len(baselineRows)+len(currentRows))
	seen := map[string]struct{}{}
	for key := range currentRows {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range baselineRows {
		if _, ok := seen[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		baseRow, hasBaseline := baselineRows[key]
		curRow, hasCurrent := currentRows[key]
		row := comparisonRow{Status: statusOK}
		switch {
		case hasCurrent:
			row.Section = curRow.Section
			row.Target = curRow.Target
			row.Current = curRow.Median
		case hasBaseline:
			row.Section = baseRow.Section
			row.Target = baseRow.Target
		}
		if hasBaseline {
			row.Baseline = baseRow.Median
		} else {
			row.MissingBaseline = true
			result.addGate(&row, opts.Strict, "matching baseline row is required", false, "current benchmark row has no baseline")
		}
		if hasCurrent {
			row.Current = curRow.Median
		} else {
			row.MissingCurrent = true
			result.addGate(&row, opts.Strict, "matching current row is required", false, "baseline benchmark row is missing from current results")
		}
		if hasBaseline && hasCurrent {
			row.PublishDeltaPct = percentDelta(row.Baseline.PublishRate, row.Current.PublishRate)
			row.ConsumeDeltaPct = percentDelta(row.Baseline.ConsumeRate, row.Current.ConsumeRate)
			row.P99DeltaPct = percentDelta(row.Baseline.LatencyP99Ms, row.Current.LatencyP99Ms)
			result.applyPhase2Gates(&row, opts.Strict)
		}
		result.Rows = append(result.Rows, row)
	}

	return result
}

type flattenedRow struct {
	Section string
	Target  string
	Median  medianResult
}

func rowsFromDocument(doc benchmarkDocument) []flattenedRow {
	if len(doc.Sections) == 0 {
		return rowsFromReport("Benchmark", benchmarkReport{Workload: doc.Workload, Summaries: doc.Summaries})
	}

	rows := make([]flattenedRow, 0)
	for _, section := range doc.Sections {
		label := section.Label
		if label == "" {
			label = section.Report.Workload.Label
		}
		if label == "" {
			label = "Benchmark"
		}
		rows = append(rows, rowsFromReport(label, section.Report)...)
	}
	return rows
}

func rowsFromReport(label string, report benchmarkReport) []flattenedRow {
	rows := make([]flattenedRow, 0, len(report.Summaries))
	for _, summary := range report.Summaries {
		median := summary.MedianResult
		if median.Target == "" {
			median.Target = summary.Target
		}
		if median.Consumers == 0 {
			median.Consumers = report.Workload.Consumers
		}
		if median.PayloadBytes == 0 {
			median.PayloadBytes = report.Workload.PayloadBytes
		}
		rows = append(rows, flattenedRow{
			Section: label,
			Target:  median.Target,
			Median:  median,
		})
	}
	return rows
}

func rowsByKey(rows []flattenedRow) map[string]flattenedRow {
	byKey := make(map[string]flattenedRow, len(rows))
	for _, row := range rows {
		byKey[rowKey(row.Section, row.Target)] = row
	}
	return byKey
}

func rowKey(section, target string) string {
	return strings.ToLower(strings.TrimSpace(section)) + "\x00" + strings.ToLower(strings.TrimSpace(target))
}

func (result *comparisonResult) applyPhase2Gates(row *comparisonRow, strict bool) {
	label := strings.ToLower(row.Section)
	if strings.Contains(label, "single consumer") && row.Baseline.ConsumeRate > 0 {
		ratio := row.Current.ConsumeRate / row.Baseline.ConsumeRate
		result.addGate(row, strict, "single-consumer consume throughput >= 2.0x baseline", ratio >= 2.0, fmt.Sprintf("consume ratio %.2fx", ratio))
	}
	if strings.Contains(label, "backlog drain") && row.Baseline.ConsumeRate > 0 {
		ratio := row.Current.ConsumeRate / row.Baseline.ConsumeRate
		result.addGate(row, strict, "backlog drain consume throughput >= 2.0x baseline", ratio >= 2.0, fmt.Sprintf("consume ratio %.2fx", ratio))
	}
	if strings.Contains(label, "competing consumers") && row.Baseline.ConsumeRate > 0 {
		ratio := row.Current.ConsumeRate / row.Baseline.ConsumeRate
		result.addGate(row, strict, "competing consumers consume throughput >= 1.5x baseline", ratio >= 1.5, fmt.Sprintf("consume ratio %.2fx", ratio))
	}
	if isSmallPayload(row.Current.PayloadBytes) && row.Baseline.PublishRate > 0 {
		ratio := row.Current.PublishRate / row.Baseline.PublishRate
		result.addGate(row, strict, "publish throughput for 64B/256B/1024B payloads must not regress by more than 10%", ratio >= 0.90, fmt.Sprintf("publish ratio %.2fx", ratio))
	}
	if row.Baseline.LatencyP99Ms > 0 {
		ratio := row.Current.LatencyP99Ms / row.Baseline.LatencyP99Ms
		result.addGate(row, strict, "p99 latency must not regress by more than 25%", ratio <= 1.25, fmt.Sprintf("p99 ratio %.2fx", ratio))
	}
	if isSingleConsumer(row) {
		result.addGate(row, strict, "FIFO violations must remain 0 for single-consumer workloads", row.Current.FIFOViolations == 0, fmt.Sprintf("current FIFO violations %d", row.Current.FIFOViolations))
	}
	if strings.Contains(label, "competing consumers") {
		allowed := float64(row.Baseline.FIFOViolations) * 1.25
		passed := row.Current.FIFOViolations == 0
		if row.Baseline.FIFOViolations > 0 {
			passed = float64(row.Current.FIFOViolations) <= allowed
		}
		result.addGate(row, strict, "competing-consumer FIFO violations must not get worse by more than 25%", passed, fmt.Sprintf("baseline %d, current %d", row.Baseline.FIFOViolations, row.Current.FIFOViolations))
	}
}

func isSmallPayload(payloadBytes int) bool {
	return payloadBytes == 64 || payloadBytes == 256 || payloadBytes == 1024
}

func isSingleConsumer(row *comparisonRow) bool {
	label := strings.ToLower(row.Section)
	return row.Current.Consumers == 1 || strings.Contains(label, "1p/1c") || strings.Contains(label, "single consumer")
}

func (result *comparisonResult) addGate(row *comparisonRow, strict bool, name string, passed bool, detail string) {
	if passed {
		return
	}
	status := statusWarn
	if strict {
		status = statusFail
	}
	row.Status = worseStatus(row.Status, status)
	row.GateDescriptions = append(row.GateDescriptions, name)
	result.Gates = append(result.Gates, gateResult{
		Status:  status,
		Gate:    name,
		Section: row.Section,
		Target:  row.Target,
		Detail:  detail,
	})
}

func worseStatus(left, right gateStatus) gateStatus {
	if left == statusFail || right == statusFail {
		return statusFail
	}
	if left == statusWarn || right == statusWarn {
		return statusWarn
	}
	return statusOK
}

func (result comparisonResult) Failed() bool {
	for _, gate := range result.Gates {
		if gate.Status == statusFail {
			return true
		}
	}
	return false
}

func renderMarkdown(result comparisonResult) string {
	var b strings.Builder
	mode := "warning"
	if result.Strict {
		mode = "strict"
	}
	fmt.Fprintf(&b, "# Benchmark Comparison\n\n")
	fmt.Fprintf(&b, "- Baseline: `%s`\n", result.BaselineName)
	fmt.Fprintf(&b, "- Current: `%s`\n", result.CurrentName)
	fmt.Fprintf(&b, "- Mode: `%s`\n\n", mode)

	b.WriteString("| Section | Target | Publish msg/s | Consume msg/s | p99 ms | FIFO violations | Status |\n")
	b.WriteString("| --- | --- | ---: | ---: | ---: | ---: | --- |\n")
	for _, row := range result.Rows {
		fmt.Fprintf(
			&b,
			"| %s | %s | %s | %s | %s | %d -> %d | %s |\n",
			escapeMarkdown(row.Section),
			escapeMarkdown(row.Target),
			formatRateDelta(row.Baseline.PublishRate, row.Current.PublishRate),
			formatRateDelta(row.Baseline.ConsumeRate, row.Current.ConsumeRate),
			formatLatencyDelta(row.Baseline.LatencyP99Ms, row.Current.LatencyP99Ms),
			row.Baseline.FIFOViolations,
			row.Current.FIFOViolations,
			row.Status,
		)
	}

	b.WriteString("\n## Gate Results\n\n")
	if len(result.Gates) == 0 {
		b.WriteString("No benchmark gate warnings.\n")
		return b.String()
	}

	b.WriteString("| Status | Gate | Section | Target | Detail |\n")
	b.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, gate := range result.Gates {
		fmt.Fprintf(
			&b,
			"| %s | %s | %s | %s | %s |\n",
			gate.Status,
			escapeMarkdown(gate.Gate),
			escapeMarkdown(gate.Section),
			escapeMarkdown(gate.Target),
			escapeMarkdown(gate.Detail),
		)
	}
	return b.String()
}

func formatRateDelta(baseline, current float64) string {
	return fmt.Sprintf("%.0f -> %.0f (%s)", baseline, current, formatPercentDelta(baseline, current))
}

func formatLatencyDelta(baseline, current float64) string {
	return fmt.Sprintf("%.2f -> %.2f (%s)", baseline, current, formatPercentDelta(baseline, current))
}

func formatPercentDelta(baseline, current float64) string {
	if baseline == 0 {
		if current == 0 {
			return "0.0%"
		}
		return "n/a"
	}
	return fmt.Sprintf("%+.1f%%", percentDelta(baseline, current))
}

func percentDelta(baseline, current float64) float64 {
	if baseline == 0 {
		return 0
	}
	return ((current - baseline) / baseline) * 100
}

func escapeMarkdown(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}
