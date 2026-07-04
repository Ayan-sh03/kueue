package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestRunCompareWritesSummaryAndWarnsWithoutFailing(t *testing.T) {
	dir := t.TempDir()
	baselinePath := filepath.Join(dir, "baseline.json")
	currentPath := filepath.Join(dir, "current.json")
	summaryPath := filepath.Join(dir, "summary.md")

	writeTestFile(t, baselinePath, benchmarkJSON(
		benchSection{Label: "Single consumer (1p/1c)", Target: "kueue", PublishRate: 1000, ConsumeRate: 1000, LatencyP99Ms: 100, FIFOViolations: 0, Consumers: 1, PayloadBytes: 256},
		benchSection{Label: "Payload 64B (1p/1c)", Target: "kueue", PublishRate: 1000, ConsumeRate: 900, LatencyP99Ms: 100, FIFOViolations: 0, Consumers: 1, PayloadBytes: 64},
	))
	writeTestFile(t, currentPath, benchmarkJSON(
		benchSection{Label: "Single consumer (1p/1c)", Target: "kueue", PublishRate: 1100, ConsumeRate: 1900, LatencyP99Ms: 120, FIFOViolations: 1, Consumers: 1, PayloadBytes: 256},
		benchSection{Label: "Payload 64B (1p/1c)", Target: "kueue", PublishRate: 850, ConsumeRate: 950, LatencyP99Ms: 105, FIFOViolations: 0, Consumers: 1, PayloadBytes: 64},
	))

	var stdout, stderr bytes.Buffer
	exitCode := runCompare([]string{
		"-baseline", baselinePath,
		"-current", currentPath,
		"-summary-out", summaryPath,
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("non-strict runCompare exit code = %d, want 0; stderr=%s", exitCode, stderr.String())
	}

	summaryBytes, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	summary := string(summaryBytes)
	for _, want := range []string{
		"| Section | Target | Publish msg/s | Consume msg/s | p99 ms | FIFO violations | Status |",
		"Single consumer (1p/1c)",
		"1000 -> 1100 (+10.0%)",
		"1000 -> 1900 (+90.0%)",
		"100.00 -> 120.00 (+20.0%)",
		"0 -> 1",
		"WARN",
		"Payload 64B (1p/1c)",
		"-15.0%",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	if stdout.String() != summary {
		t.Fatalf("stdout summary and summary file differ\nstdout:\n%s\nfile:\n%s", stdout.String(), summary)
	}
}

func TestRunCompareFailsStrictPhase2Gates(t *testing.T) {
	dir := t.TempDir()
	baselinePath := filepath.Join(dir, "baseline.json")
	currentPath := filepath.Join(dir, "current.json")

	writeTestFile(t, baselinePath, benchmarkJSON(
		benchSection{Label: "Single consumer (1p/1c)", Target: "kueue", PublishRate: 1000, ConsumeRate: 1000, LatencyP99Ms: 100, FIFOViolations: 0, Consumers: 1, PayloadBytes: 256},
		benchSection{Label: "Backlog drain", Target: "kueue", PublishRate: 0, ConsumeRate: 1000, LatencyP99Ms: 100, FIFOViolations: 0, Consumers: 10, PayloadBytes: 256},
		benchSection{Label: "Competing consumers (1p/10c)", Target: "kueue", PublishRate: 1000, ConsumeRate: 1000, LatencyP99Ms: 100, FIFOViolations: 4, Consumers: 10, PayloadBytes: 256},
		benchSection{Label: "Payload 64B (1p/1c)", Target: "kueue", PublishRate: 1000, ConsumeRate: 1000, LatencyP99Ms: 100, FIFOViolations: 0, Consumers: 1, PayloadBytes: 64},
	))
	writeTestFile(t, currentPath, benchmarkJSON(
		benchSection{Label: "Single consumer (1p/1c)", Target: "kueue", PublishRate: 950, ConsumeRate: 1900, LatencyP99Ms: 126, FIFOViolations: 1, Consumers: 1, PayloadBytes: 256},
		benchSection{Label: "Backlog drain", Target: "kueue", PublishRate: 0, ConsumeRate: 1999, LatencyP99Ms: 100, FIFOViolations: 0, Consumers: 10, PayloadBytes: 256},
		benchSection{Label: "Competing consumers (1p/10c)", Target: "kueue", PublishRate: 1000, ConsumeRate: 1499, LatencyP99Ms: 100, FIFOViolations: 6, Consumers: 10, PayloadBytes: 256},
		benchSection{Label: "Payload 64B (1p/1c)", Target: "kueue", PublishRate: 899, ConsumeRate: 1200, LatencyP99Ms: 100, FIFOViolations: 0, Consumers: 1, PayloadBytes: 64},
	))

	var stdout, stderr bytes.Buffer
	exitCode := runCompare([]string{
		"-baseline", baselinePath,
		"-current", currentPath,
		"-strict",
	}, &stdout, &stderr)
	if exitCode != 1 {
		t.Fatalf("strict runCompare exit code = %d, want 1; stderr=%s", exitCode, stderr.String())
	}

	summary := stdout.String()
	for _, want := range []string{
		"FAIL",
		"single-consumer consume throughput >= 2.0x baseline",
		"backlog drain consume throughput >= 2.0x baseline",
		"competing consumers consume throughput >= 1.5x baseline",
		"publish throughput for 64B/256B/1024B payloads must not regress by more than 10%",
		"p99 latency must not regress by more than 25%",
		"FIFO violations must remain 0 for single-consumer workloads",
		"competing-consumer FIFO violations must not get worse by more than 25%",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("strict summary missing %q:\n%s", want, summary)
		}
	}
}

type benchSection struct {
	Label          string
	Target         string
	PublishRate    float64
	ConsumeRate    float64
	LatencyP99Ms   float64
	FIFOViolations int
	Consumers      int
	PayloadBytes   int
}

func benchmarkJSON(sections ...benchSection) string {
	var b strings.Builder
	b.WriteString(`{"sections":[`)
	for i, section := range sections {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"label":`)
		b.WriteString(strconvQuote(section.Label))
		b.WriteString(`,"report":{"summaries":[{"target":`)
		b.WriteString(strconvQuote(section.Target))
		b.WriteString(`,"medianResult":{`)
		b.WriteString(`"target":`)
		b.WriteString(strconvQuote(section.Target))
		b.WriteString(`,"publishRate":`)
		b.WriteString(formatTestFloat(section.PublishRate))
		b.WriteString(`,"consumeRate":`)
		b.WriteString(formatTestFloat(section.ConsumeRate))
		b.WriteString(`,"latencyP99Ms":`)
		b.WriteString(formatTestFloat(section.LatencyP99Ms))
		b.WriteString(`,"fifoViolations":`)
		b.WriteString(formatTestInt(section.FIFOViolations))
		b.WriteString(`,"consumers":`)
		b.WriteString(formatTestInt(section.Consumers))
		b.WriteString(`,"payloadBytes":`)
		b.WriteString(formatTestInt(section.PayloadBytes))
		b.WriteString(`}}]}}`)
	}
	b.WriteString(`]}`)
	return b.String()
}

func writeTestFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func strconvQuote(value string) string {
	return strconv.Quote(value)
}

func formatTestFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func formatTestInt(value int) string {
	return strconv.Itoa(value)
}
