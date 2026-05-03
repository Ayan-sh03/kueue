package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var errNoMessage = errors.New("no message available")

var debugLog bool

func debugf(format string, args ...any) {
	if debugLog {
		fmt.Fprintf(os.Stderr, "[bench] "+format+"\n", args...)
	}
}

type workloadConfig struct {
	Messages        int      `json:"messages"`
	Warmup          int      `json:"warmup"`
	Runs            int      `json:"runs"`
	PayloadBytes    int      `json:"payloadBytes"`
	Producers       int      `json:"producers"`
	Consumers       int      `json:"consumers"`
	Prefetch        int      `json:"prefetch"`
	Targets         []string `json:"targets"`
	KueueBatchSize  int      `json:"kueueBatchSize"`
	RateLimit       int      `json:"rateLimit"`        // msg/s, 0 = unlimited
	ConsumerDelayMs int      `json:"consumerDelayMs"`  // artificial per-msg delay, 0 = none
	VerifyOrder     bool     `json:"verifyOrder"`       // verify FIFO within consumer
	PreFill         int      `json:"preFill"`           // pre-fill N messages before consumers start
	Label           string   `json:"label"`             // human-readable section label
}

type benchmarkPayload struct {
	Seq            int    `json:"seq"`
	ProducerID     int    `json:"producerId"`
	SentAtUnixNano int64  `json:"sentAtUnixNano"`
	Padding        string `json:"padding,omitempty"`
}

type benchmarkResult struct {
	Target              string  `json:"target"`
	Run                 int     `json:"run"`
	Messages            int     `json:"messages"`
	PayloadBytes        int     `json:"payloadBytes"`
	Producers           int     `json:"producers"`
	Consumers           int     `json:"consumers"`
	PublishSeconds      float64 `json:"publishSeconds"`
	ConsumeSeconds      float64 `json:"consumeSeconds"`
	EndToEndSeconds     float64 `json:"endToEndSeconds"`
	PublishRate         float64 `json:"publishRate"`
	ConsumeRate         float64 `json:"consumeRate"`
	LatencyP50Ms        float64 `json:"latencyP50Ms"`
	LatencyP95Ms        float64 `json:"latencyP95Ms"`
	LatencyP99Ms        float64 `json:"latencyP99Ms"`
	LatencyP999Ms       float64 `json:"latencyP999Ms"`
	LatencyMaxMs        float64 `json:"latencyMaxMs"`
	PublishedMessages   int64   `json:"publishedMessages"`
	ConsumedMessages    int64   `json:"consumedMessages"`
	FIFOViolations      int     `json:"fifoViolations"`
	ConsumerCountStdDev float64 `json:"consumerCountStdDev"`
	ConsumerCountMin    int64   `json:"consumerCountMin"`
	ConsumerCountMax    int64   `json:"consumerCountMax"`
}

type benchmarkSummary struct {
	Target       string            `json:"target"`
	Runs         []benchmarkResult `json:"runs"`
	MedianResult benchmarkResult   `json:"medianResult"`
}

type benchmarkReport struct {
	GeneratedAt time.Time          `json:"generatedAt"`
	Workload    workloadConfig     `json:"workload"`
	Summaries   []benchmarkSummary `json:"summaries"`
}

type delivery struct {
	ID   string
	Body []byte
	Ack  func(context.Context) error
}

type publisher interface {
	Publish(context.Context, []byte) error
	Close() error
}

type benchmarkTarget interface {
	Name() string
	Setup(context.Context, workloadConfig, string) error
	NewPublisher(int) (publisher, error)
	Consume(context.Context) (*delivery, error)
	Cleanup(context.Context) error
}

func main() {
	var (
		targets       = flag.String("targets", "kueue", "comma-separated benchmark targets")
		kueueURL      = flag.String("kueue-url", "http://127.0.0.1:8080", "base URL for the kueue HTTP server")
		rabbitMQURI   = flag.String("rabbitmq-uri", "amqp://guest:guest@127.0.0.1:5672/", "AMQP URI for RabbitMQ")
		messages      = flag.Int("messages", 10000, "messages per measured run")
		warmup        = flag.Int("warmup", 500, "warmup messages before each measured run")
		runs          = flag.Int("runs", 3, "measured runs per target")
		payloadBytes  = flag.Int("payload-bytes", 256, "message body size in bytes")
		producers     = flag.Int("producers", 1, "concurrent publishers")
		consumers     = flag.Int("consumers", 1, "concurrent consumers")
		prefetch      = flag.Int("prefetch", 200, "RabbitMQ consumer prefetch per consumer")
		rateLimit     = flag.Int("rate", 0, "max publish rate in msg/s (0=unlimited)")
		consumerDelay = flag.Int("consumer-delay", 0, "artificial per-message consumer delay in ms (0=none)")
		verifyOrder   = flag.Bool("verify-order", false, "verify FIFO ordering within each consumer")
		workload      = flag.String("workload", "default", "workload preset: default, competing-consumers, backlog-drain, size-sweep, full")
		jsonOut       = flag.String("json-out", "", "optional path for benchmark report JSON")
	)
	flag.Parse()

	debugLog = os.Getenv("DEBUG") == "true"

	targetList := parseTargets(*targets)
	debugf("targets: %v, messages: %d, runs: %d, workload: %s", targetList, *messages, *runs, *workload)

	var sections []sectionReport

	switch *workload {
	case "default":
		e2eCfg := workloadConfig{
			Messages:       *messages,
			Warmup:         *warmup,
			Runs:           *runs,
			PayloadBytes:   *payloadBytes,
			Producers:      *producers,
			Consumers:      *consumers,
			Prefetch:       *prefetch,
			Targets:        targetList,
			KueueBatchSize: 10,
			RateLimit:      *rateLimit,
			ConsumerDelayMs: *consumerDelay,
			VerifyOrder:    *verifyOrder,
			Label:          "End-to-end (default client config)",
		}
		sections = append(sections, runSection("End-to-end (default client config)", targetList, e2eCfg, *kueueURL, *rabbitMQURI, "Note: measures protocol + broker together."))

		applesCfg := e2eCfg
		applesCfg.Prefetch = 1
		applesCfg.KueueBatchSize = 1
		applesCfg.Label = "Apples-to-apples (one message per round-trip)"
		sections = append(sections, runSection("Apples-to-apples (one message per round-trip)", targetList, applesCfg, *kueueURL, *rabbitMQURI, "Note: isolates broker behavior; not a realistic config for either."))

	case "competing-consumers":
		cfg := workloadConfig{
			Messages:       *messages,
			Warmup:         *warmup,
			Runs:           *runs,
			PayloadBytes:   *payloadBytes,
			Producers:      1,
			Consumers:      10,
			Prefetch:       *prefetch,
			Targets:        targetList,
			KueueBatchSize: 10,
			VerifyOrder:    true,
			Label:          "Competing consumers (1p/10c)",
		}
		sections = append(sections, runSection("Competing consumers (1p/10c)", targetList, cfg, *kueueURL, *rabbitMQURI, "Note: measures throughput scaling, FIFO ordering, and consumer fairness."))

	case "backlog-drain":
		cfg := workloadConfig{
			Messages:       *messages,
			Warmup:         0,
			Runs:           *runs,
			PayloadBytes:   *payloadBytes,
			Producers:      *producers,
			Consumers:      *consumers,
			Prefetch:       *prefetch,
			Targets:        targetList,
			KueueBatchSize: 10,
			PreFill:        *messages,
			VerifyOrder:    *verifyOrder,
			Label:          "Backlog drain",
		}
		sections = append(sections, runSection("Backlog drain", targetList, cfg, *kueueURL, *rabbitMQURI, "Note: pre-fills queue then measures drain rate and latency under backlog."))

	case "size-sweep":
		sizes := []int{64, 256, 1024, 4096, 16384}
		for _, sz := range sizes {
			cfg := workloadConfig{
				Messages:       *messages,
				Warmup:         *warmup,
				Runs:           *runs,
				PayloadBytes:   sz,
				Producers:      *producers,
				Consumers:      *consumers,
				Prefetch:       *prefetch,
				Targets:        targetList,
				KueueBatchSize: 10,
				VerifyOrder:    *verifyOrder,
				Label:          fmt.Sprintf("Size sweep %dB", sz),
			}
			sections = append(sections, runSection(fmt.Sprintf("Payload %dB", sz), targetList, cfg, *kueueURL, *rabbitMQURI, ""))
		}

	case "full":
		e2eCfg := workloadConfig{
			Messages:       *messages,
			Warmup:         *warmup,
			Runs:           *runs,
			PayloadBytes:   *payloadBytes,
			Producers:      1,
			Consumers:      1,
			Prefetch:       *prefetch,
			Targets:        targetList,
			KueueBatchSize: 10,
			VerifyOrder:    true,
			Label:          "Single consumer (1p/1c)",
		}
		sections = append(sections, runSection("Single consumer (1p/1c)", targetList, e2eCfg, *kueueURL, *rabbitMQURI, ""))

		ccCfg := workloadConfig{
			Messages:       *messages,
			Warmup:         *warmup,
			Runs:           *runs,
			PayloadBytes:   *payloadBytes,
			Producers:      1,
			Consumers:      10,
			Prefetch:       *prefetch,
			Targets:        targetList,
			KueueBatchSize: 10,
			VerifyOrder:    true,
			Label:          "Competing consumers (1p/10c)",
		}
		sections = append(sections, runSection("Competing consumers (1p/10c)", targetList, ccCfg, *kueueURL, *rabbitMQURI, ""))

		for _, sz := range []int{64, 1024, 16384} {
			cfg := workloadConfig{
				Messages:       *messages,
				Warmup:         *warmup,
				Runs:           *runs,
				PayloadBytes:   sz,
				Producers:      1,
				Consumers:      1,
				Prefetch:       *prefetch,
				Targets:        targetList,
				KueueBatchSize: 10,
				VerifyOrder:    true,
				Label:          fmt.Sprintf("Payload %dB (1p/1c)", sz),
			}
			sections = append(sections, runSection(fmt.Sprintf("Payload %dB", sz), targetList, cfg, *kueueURL, *rabbitMQURI, ""))
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown workload %q; use one of: default, competing-consumers, backlog-drain, size-sweep, full\n", *workload)
		os.Exit(1)
	}

	for _, s := range sections {
		printSection(s)
	}

	if *jsonOut != "" {
		sectionOutput := func(sr sectionReport) map[string]any {
			return map[string]any{
				"label":  sr.Label,
				"note":   sr.Note,
				"report": sr.Report,
			}
		}
		outSections := make([]any, len(sections))
		for i, s := range sections {
			outSections[i] = sectionOutput(s)
		}
		data, err := json.MarshalIndent(map[string]any{
			"generatedAt": time.Now().UTC(),
			"sections":    outSections,
		}, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal report: %v\n", err)
			os.Exit(1)
		}
		if err := os.MkdirAll(filepathDir(*jsonOut), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "create report dir: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*jsonOut, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(1)
		}
	}
}

type sectionReport struct {
	Label  string
	Note   string
	Report benchmarkReport
}

func runSection(label string, targetList []string, cfg workloadConfig, kueueURL, rabbitMQURI, note string) sectionReport {
	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "%s: invalid config: %v\n", label, err)
		os.Exit(1)
	}

	debugf("--- section: %s (batch=%d, prefetch=%d) ---", label, cfg.KueueBatchSize, cfg.Prefetch)

	report := benchmarkReport{
		GeneratedAt: time.Now().UTC(),
		Workload:    cfg,
	}

	for _, name := range targetList {
		debugf("target: %s", name)
		target, err := newTarget(name, kueueURL, rabbitMQURI)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: target %q: %v\n", label, name, err)
			os.Exit(1)
		}

		summary, err := runTarget(context.Background(), target, cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s benchmark failed: %v\n", label, target.Name(), err)
			os.Exit(1)
		}

		report.Summaries = append(report.Summaries, summary)
	}

	return sectionReport{Label: label, Note: note, Report: report}
}

func printSection(s sectionReport) {
	fmt.Printf("\n=== %s ===\n", s.Label)
	printReport(s.Report)
	fmt.Printf("--- %s ---\n", s.Note)
}

func parseTargets(raw string) []string {
	parts := strings.Split(raw, ",")
	targets := make([]string, 0, len(parts))
	seen := map[string]struct{}{}

	for _, part := range parts {
		name := strings.TrimSpace(strings.ToLower(part))
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		targets = append(targets, name)
	}

	return targets
}

func validateConfig(cfg workloadConfig) error {
	switch {
	case len(cfg.Targets) == 0:
		return errors.New("at least one target is required")
	case cfg.Messages <= 0:
		return errors.New("messages must be > 0")
	case cfg.Warmup < 0:
		return errors.New("warmup must be >= 0")
	case cfg.Runs <= 0:
		return errors.New("runs must be > 0")
	case cfg.PayloadBytes <= 0:
		return errors.New("payload-bytes must be > 0")
	case cfg.Producers <= 0:
		return errors.New("producers must be > 0")
	case cfg.Consumers <= 0:
		return errors.New("consumers must be > 0")
	case cfg.Prefetch <= 0:
		return errors.New("prefetch must be > 0")
	case cfg.RateLimit < 0:
		return errors.New("rate must be >= 0")
	case cfg.ConsumerDelayMs < 0:
		return errors.New("consumer-delay must be >= 0")
	default:
		return nil
	}
}

func newTarget(name, kueueURL, rabbitMQURI string) (benchmarkTarget, error) {
	switch name {
	case "kueue":
		return newKueueTarget(kueueURL), nil
	case "rabbitmq":
		return newRabbitMQTarget(rabbitMQURI), nil
	default:
		return nil, fmt.Errorf("unsupported target %q", name)
	}
}

func runTarget(ctx context.Context, target benchmarkTarget, cfg workloadConfig) (benchmarkSummary, error) {
	summary := benchmarkSummary{Target: target.Name()}

	fmt.Printf("\n== %s ==\n", strings.ToUpper(target.Name()))

	for run := 1; run <= cfg.Runs; run++ {
		debugf("setup run %d/%d", run, cfg.Runs)
		runLabel := fmt.Sprintf("%s-%d-%d", target.Name(), run, time.Now().UnixNano())
		if err := target.Setup(ctx, cfg, runLabel); err != nil {
			return summary, err
		}

		if cfg.Warmup > 0 {
			debugf("warmup: %d messages...", cfg.Warmup)
			if _, err := executeWorkload(ctx, target, cfg, cfg.Warmup); err != nil {
				_ = target.Cleanup(context.Background())
				return summary, fmt.Errorf("warmup run %d: %w", run, err)
			}
		}

		debugf("measured run %d: %d messages...", run, cfg.Messages)
		result, err := executeWorkload(ctx, target, cfg, cfg.Messages)
		debugf("cleanup...")
		cleanupErr := target.Cleanup(context.Background())
		if err != nil {
			return summary, fmt.Errorf("measured run %d: %w", run, err)
		}
		if cleanupErr != nil {
			return summary, fmt.Errorf("cleanup run %d: %w", run, cleanupErr)
		}

		result.Target = target.Name()
		result.Run = run
		summary.Runs = append(summary.Runs, result)

		fmt.Printf(
			"run %d: publish %.0f msg/s, consume %.0f msg/s, latency p50 %.2f ms, p95 %.2f ms, p99 %.2f ms, p99.9 %.2f ms",
			run,
			result.PublishRate,
			result.ConsumeRate,
			result.LatencyP50Ms,
			result.LatencyP95Ms,
			result.LatencyP99Ms,
			result.LatencyP999Ms,
		)
		if result.FIFOViolations > 0 {
			fmt.Printf(", FIFO violations %d", result.FIFOViolations)
		}
		if result.Consumers > 1 {
			fmt.Printf(", fairness std %.1f (%d-%d)", result.ConsumerCountStdDev, result.ConsumerCountMin, result.ConsumerCountMax)
		}
		fmt.Println()
	}

	summary.MedianResult = medianResult(summary.Target, summary.Runs)
	return summary, nil
}

func executeWorkload(parent context.Context, target benchmarkTarget, cfg workloadConfig, messages int) (benchmarkResult, error) {
	runCtx, cancelRun := context.WithTimeout(parent, 10*time.Minute)
	defer cancelRun()
	consumeCtx, cancelConsume := context.WithCancel(runCtx)
	defer cancelConsume()

	result := benchmarkResult{
		Messages:     messages,
		PayloadBytes: cfg.PayloadBytes,
		Producers:    cfg.Producers,
		Consumers:    cfg.Consumers,
	}

	var (
		latencyMu      sync.Mutex
		latencies      []time.Duration
		produced       atomic.Int64
		consumed       atomic.Int64
		firstErr       error
		errOnce        sync.Once
		consumerCounts []atomic.Int64
		fifoViolations atomic.Int64
		seqCh          chan int
	)

	consumerCounts = make([]atomic.Int64, cfg.Consumers)

	if cfg.PreFill == 0 {
		seqCh = make(chan int, messages)
	}

	recordErr := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancelRun()
		})
	}

	doneCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				debugf("progress: produced=%d/%d consumed=%d/%d", produced.Load(), messages, consumed.Load(), messages)
			case <-doneCh:
				return
			}
		}
	}()
	defer close(doneCh)

	if cfg.PreFill > 0 {
		prefillCtx, prefillCancel := context.WithTimeout(runCtx, 5*time.Minute)
		prefillDone := make(chan struct{})
		go func() {
			defer close(prefillDone)
			pub, err := target.NewPublisher(0)
			if err != nil {
				recordErr(err)
				return
			}
			for seq := 0; seq < cfg.PreFill; seq++ {
				body, err := makePayload(seq, 0, cfg.PayloadBytes)
				if err != nil {
					recordErr(err)
					return
				}
				if err := pub.Publish(prefillCtx, body); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return
					}
					recordErr(err)
					return
				}
				produced.Add(1)
			}
			if err := pub.Close(); err != nil {
				recordErr(err)
			}
		}()
		<-prefillDone
		prefillCancel()
		if firstErr != nil {
			return result, firstErr
		}
		debugf("pre-filled %d messages, starting consumers", cfg.PreFill)
	} else {
		for seq := 0; seq < messages; seq++ {
			seqCh <- seq
		}
		close(seqCh)
	}

	totalToConsume := messages
	if cfg.PreFill > 0 {
		totalToConsume = cfg.PreFill
	}

	consumeStarted := time.Now()
	var consumerWG sync.WaitGroup
	for i := 0; i < cfg.Consumers; i++ {
		consumerIdx := i
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			var lastSeq int = -1
			lastSeqByProducer := make(map[int]int)
			for {
				if consumed.Load() >= int64(totalToConsume) {
					return
				}

				msg, err := target.Consume(consumeCtx)
				if err != nil {
					if errors.Is(err, errNoMessage) {
						continue
					}
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return
					}
					recordErr(err)
					return
				}

				latency, err := parseLatency(msg.Body)
				if err != nil {
					recordErr(err)
					return
				}

				if cfg.ConsumerDelayMs > 0 {
					time.Sleep(time.Duration(cfg.ConsumerDelayMs) * time.Millisecond)
				}

				if err := msg.Ack(runCtx); err != nil {
					recordErr(err)
					return
				}

				latencyMu.Lock()
				latencies = append(latencies, latency)
				latencyMu.Unlock()

				if cfg.VerifyOrder {
					var payload benchmarkPayload
					if jsonErr := json.Unmarshal(msg.Body, &payload); jsonErr == nil {
						if cfg.Producers == 1 {
							if lastSeq >= 0 && payload.Seq < lastSeq {
								fifoViolations.Add(1)
							}
							lastSeq = payload.Seq
						} else {
							prev, ok := lastSeqByProducer[payload.ProducerID]
							if ok && payload.Seq < prev {
								fifoViolations.Add(1)
							}
							lastSeqByProducer[payload.ProducerID] = payload.Seq
						}
					}
				}

				consumerCounts[consumerIdx].Add(1)
				if consumed.Add(1) >= int64(totalToConsume) {
					cancelConsume()
					return
				}
			}
		}()
	}

	if cfg.PreFill == 0 {
		publishStarted := time.Now()
		var producerWG sync.WaitGroup
		for i := 0; i < cfg.Producers; i++ {
			pub, err := target.NewPublisher(i)
			if err != nil {
				cancelRun()
				consumerWG.Wait()
				return result, err
			}

			producerID := i
			producerWG.Add(1)
			go func(p publisher, pid int) {
				defer producerWG.Done()
				defer func() {
				if err := p.Close(); err != nil {
					recordErr(err)
				}
			}()

				var rateLimiter *time.Ticker
				if cfg.RateLimit > 0 {
					interval := time.Duration(float64(time.Second) / float64(cfg.RateLimit))
					rateLimiter = time.NewTicker(interval)
					defer rateLimiter.Stop()
				}

				for seq := range seqCh {
					if rateLimiter != nil {
						select {
						case <-rateLimiter.C:
						case <-runCtx.Done():
							return
						}
					}

					body, err := makePayload(seq, pid, cfg.PayloadBytes)
					if err != nil {
						recordErr(err)
						return
					}

					if err := p.Publish(runCtx, body); err != nil {
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
						recordErr(err)
						return
					}

					produced.Add(1)
				}
			}(pub, producerID)
		}

		producerWG.Wait()
		result.PublishSeconds = time.Since(publishStarted).Seconds()
	}

	consumerWG.Wait()
	consumeDone := time.Since(consumeStarted)

	if firstErr != nil {
		return result, firstErr
	}
	msgCount := int64(messages)
	if cfg.PreFill > 0 {
		msgCount = int64(totalToConsume)
	}
	if produced.Load() != msgCount && cfg.PreFill == 0 {
		return result, fmt.Errorf("published %d/%d messages", produced.Load(), msgCount)
	}
	if consumed.Load() != msgCount {
		return result, fmt.Errorf("consumed %d/%d messages", consumed.Load(), msgCount)
	}
	expectedLatencies := int(messages)
	if cfg.PreFill > 0 {
		expectedLatencies = totalToConsume
	}
	if len(latencies) != expectedLatencies {
		return result, fmt.Errorf("recorded %d/%d latencies", len(latencies), expectedLatencies)
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	result.ConsumeSeconds = consumeDone.Seconds()
	result.EndToEndSeconds = consumeDone.Seconds()
	if cfg.PreFill == 0 && result.PublishSeconds > 0 {
		result.PublishRate = float64(messages) / result.PublishSeconds
	}
	result.ConsumeRate = float64(msgCount) / result.ConsumeSeconds
	result.LatencyP50Ms = percentileMs(latencies, 0.50)
	result.LatencyP95Ms = percentileMs(latencies, 0.95)
	result.LatencyP99Ms = percentileMs(latencies, 0.99)
	result.LatencyP999Ms = percentileMs(latencies, 0.999)
	result.LatencyMaxMs = latencyToMs(latencies[len(latencies)-1])
	result.PublishedMessages = produced.Load()
	result.ConsumedMessages = consumed.Load()
	result.FIFOViolations = int(fifoViolations.Load())

	counts := make([]int64, cfg.Consumers)
	for i := range consumerCounts {
		counts[i] = consumerCounts[i].Load()
	}
	result.ConsumerCountMin, result.ConsumerCountMax = minMaxInt64(counts)
	result.ConsumerCountStdDev = stdDevInt64(counts)

	return result, nil
}

func makePayload(seq, producerID, payloadBytes int) ([]byte, error) {
	payload := benchmarkPayload{
		Seq:            seq,
		ProducerID:     producerID,
		SentAtUnixNano: time.Now().UnixNano(),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	if len(body) >= payloadBytes {
		return body, nil
	}

	payload.Padding = strings.Repeat("x", payloadBytes-len(body))
	body, err = json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	for len(body) < payloadBytes {
		payload.Padding += strings.Repeat("x", payloadBytes-len(body))
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}

	if len(body) > payloadBytes {
		excess := len(body) - payloadBytes
		if excess < len(payload.Padding) {
			payload.Padding = payload.Padding[:len(payload.Padding)-excess]
			return json.Marshal(payload)
		}
	}

	return body, nil
}

func parseLatency(body []byte) (time.Duration, error) {
	var payload benchmarkPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("decode payload: %w", err)
	}
	if payload.SentAtUnixNano == 0 {
		return 0, errors.New("payload missing sentAtUnixNano")
	}
	return time.Since(time.Unix(0, payload.SentAtUnixNano)), nil
}

func percentileMs(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * percentile)
	return latencyToMs(values[index])
}

func latencyToMs(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func minMaxInt64(vals []int64) (int64, int64) {
	if len(vals) == 0 {
		return 0, 0
	}
	minVal, maxVal := vals[0], vals[0]
	for _, v := range vals[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	return minVal, maxVal
}

func stdDevInt64(vals []int64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += float64(v)
	}
	mean := sum / float64(len(vals))
	var sumSqDiff float64
	for _, v := range vals {
		diff := float64(v) - mean
		sumSqDiff += diff * diff
	}
	return math.Sqrt(sumSqDiff / float64(len(vals)))
}

func medianResult(target string, runs []benchmarkResult) benchmarkResult {
	if len(runs) == 0 {
		return benchmarkResult{Target: target}
	}

	return benchmarkResult{
		Target:              target,
		Run:                 0,
		Messages:            runs[0].Messages,
		PayloadBytes:        runs[0].PayloadBytes,
		Producers:           runs[0].Producers,
		Consumers:           runs[0].Consumers,
		PublishSeconds:      medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.PublishSeconds })),
		ConsumeSeconds:      medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.ConsumeSeconds })),
		EndToEndSeconds:     medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.EndToEndSeconds })),
		PublishRate:         medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.PublishRate })),
		ConsumeRate:         medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.ConsumeRate })),
		LatencyP50Ms:        medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyP50Ms })),
		LatencyP95Ms:        medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyP95Ms })),
		LatencyP99Ms:        medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyP99Ms })),
		LatencyP999Ms:       medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyP999Ms })),
		LatencyMaxMs:        medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyMaxMs })),
		PublishedMessages:   runs[0].PublishedMessages,
		ConsumedMessages:    runs[0].ConsumedMessages,
		FIFOViolations:      runs[len(runs)/2].FIFOViolations,
		ConsumerCountStdDev: medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.ConsumerCountStdDev })),
		ConsumerCountMin:    runs[len(runs)/2].ConsumerCountMin,
		ConsumerCountMax:    runs[len(runs)/2].ConsumerCountMax,
	}
}

func extractMetric(runs []benchmarkResult, getter func(benchmarkResult) float64) []float64 {
	values := make([]float64, 0, len(runs))
	for _, run := range runs {
		values = append(values, getter(run))
	}
	return values
}

func medianFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return values[len(values)/2]
}

func printReport(report benchmarkReport) {
	fmt.Println("\n== Summary ==")
	fmt.Printf("%-10s %-12s %-12s %-10s %-10s %-10s %-10s %-12s\n", "target", "pub msg/s", "con msg/s", "p50 ms", "p95 ms", "p99 ms", "p99.9 ms", "max ms")
	for _, summary := range report.Summaries {
		median := summary.MedianResult
		fmt.Printf(
			"%-10s %-12.0f %-12.0f %-10.2f %-10.2f %-10.2f %-10.2f %-12.2f\n",
			summary.Target,
			median.PublishRate,
			median.ConsumeRate,
			median.LatencyP50Ms,
			median.LatencyP95Ms,
			median.LatencyP99Ms,
			median.LatencyP999Ms,
			median.LatencyMaxMs,
		)
	}
	for _, summary := range report.Summaries {
		median := summary.MedianResult
		extras := []string{}
		if median.FIFOViolations > 0 {
			extras = append(extras, fmt.Sprintf("FIFO violations: %d", median.FIFOViolations))
		}
		if median.Consumers > 1 {
			extras = append(extras, fmt.Sprintf("consumer fairness: std=%.1f range=[%d,%d]", median.ConsumerCountStdDev, median.ConsumerCountMin, median.ConsumerCountMax))
		}
		if len(extras) > 0 {
			fmt.Printf("  %s: %s\n", summary.Target, strings.Join(extras, ", "))
		}
	}
}

func writeReport(path string, report benchmarkReport) error {
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o644)
}

func filepathDir(path string) string {
	lastSlash := strings.LastIndexAny(path, `/\`)
	if lastSlash == -1 {
		return "."
	}
	if lastSlash == 0 {
		return path[:1]
	}
	return path[:lastSlash]
}

type receivedMsg struct {
	ID            string
	Body          []byte
	ReceiptHandle string
	DeliveryToken string
}

type ackEntryBench struct {
	ReceiptHandle string `json:"receiptHandle"`
	DeliveryToken string `json:"deliveryToken"`
}

type kueueTarget struct {
	baseURL      string
	client       *http.Client
	queueID      string
	prefetch     int
	deliveries   chan *delivery
	ackBatch     []ackEntryBench
	ackMu        sync.Mutex
	ackFlushSize int
	ackFlushTick *time.Ticker
	ackFlushStop chan struct{}
	ackFlushErr  atomic.Pointer[error]
	cancelCtx    context.CancelFunc
}

func newKueueTarget(baseURL string) *kueueTarget {
	return &kueueTarget{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: 40 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		ackFlushSize: 100,
		ackFlushStop: make(chan struct{}),
	}
}

func (t *kueueTarget) Name() string {
	return "kueue"
}

func (t *kueueTarget) Setup(ctx context.Context, cfg workloadConfig, runLabel string) error {
	type createRequest struct {
		Name       string `json:"name"`
		MaxRetries int    `json:"maxRetries"`
	}
	type createResponse struct {
		ID string `json:"id"`
	}

	var resp createResponse
	if err := t.postJSON(ctx, "/create", createRequest{
		Name:       runLabel,
		MaxRetries: 3,
	}, &resp); err != nil {
		return err
	}

	t.queueID = resp.ID
	consumerCtx, cancelConsumerCtx := context.WithCancel(ctx)
	t.cancelCtx = cancelConsumerCtx
	t.prefetch = cfg.Prefetch
	if cfg.KueueBatchSize > 0 {
		t.prefetch = cfg.KueueBatchSize
	}
	if t.prefetch <= 0 {
		t.prefetch = 1
	}
	t.deliveries = make(chan *delivery, cfg.Prefetch*cfg.Consumers)
	if cap(t.deliveries) == 0 {
		t.deliveries = make(chan *delivery, t.prefetch)
	}
	t.ackBatch = make([]ackEntryBench, 0, t.ackFlushSize)
	t.ackFlushStop = make(chan struct{})
	out := t.deliveries

	t.ackFlushTick = time.NewTicker(5 * time.Millisecond)
	go func() {
		for {
			select {
			case <-t.ackFlushTick.C:
			case <-t.ackFlushStop:
				return
			}
			t.ackMu.Lock()
			hasBatch := len(t.ackBatch) > 0
			t.ackMu.Unlock()
			if hasBatch {
				if err := t.flushAcks(context.Background()); err != nil {
					t.ackFlushErr.Store(&err)
				}
			}
		}
	}()

	consumerCount := cfg.Consumers
	if consumerCount <= 0 {
		consumerCount = 1
	}
	for i := 0; i < consumerCount; i++ {
		go func() {
			for {
				msgs, err := t.receiveBatch(consumerCtx, t.prefetch)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return
					}
					select {
					case <-time.After(100 * time.Millisecond):
						continue
					case <-consumerCtx.Done():
						return
					}
				}
				if len(msgs) == 0 {
					select {
					case <-time.After(10 * time.Millisecond):
						continue
					case <-consumerCtx.Done():
						return
					}
				}
				for _, d := range msgs {
					select {
					case out <- d:
					case <-consumerCtx.Done():
						return
					}
				}
			}
		}()
	}
	return nil
}

func (t *kueueTarget) NewPublisher(int) (publisher, error) {
	return &kueuePublisher{target: t, flushSize: 50}, nil
}

func (t *kueueTarget) Consume(ctx context.Context) (*delivery, error) {
	if errPtr := t.ackFlushErr.Load(); errPtr != nil {
		return nil, *errPtr
	}
	select {
	case msg, ok := <-t.deliveries:
		if !ok {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, io.EOF
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *kueueTarget) Cleanup(ctx context.Context) error {
	if t.cancelCtx != nil {
		t.cancelCtx()
		t.cancelCtx = nil
	}
	close(t.ackFlushStop)
	if t.ackFlushTick != nil {
		t.ackFlushTick.Stop()
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = t.flushAcks(flushCtx)
	t.queueID = ""
	return nil
}

func (t *kueueTarget) receiveBatch(ctx context.Context, max int) ([]*delivery, error) {
	url := fmt.Sprintf("%s/receive-batch?id=%s&max=%d&wait=true", t.baseURL, t.queueID, max)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := t.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var payload struct {
			Messages []struct {
				ID            string `json:"id"`
				Body          []byte `json:"body"`
				ReceiptHandle string `json:"receiptHandle"`
				DeliveryToken string `json:"deliveryToken"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, err
		}
		if len(payload.Messages) == 0 {
			return nil, errNoMessage
		}
		deliveries := make([]*delivery, len(payload.Messages))
		for i, msg := range payload.Messages {
			msgCopy := msg
			deliveries[i] = &delivery{
				ID:   msgCopy.ID,
				Body: msgCopy.Body,
				Ack: func(ctx context.Context) error {
					return t.queueAck(ctx, ackEntryBench{
						ReceiptHandle: msgCopy.ReceiptHandle,
						DeliveryToken: msgCopy.DeliveryToken,
					})
				},
			}
		}
		return deliveries, nil
	case http.StatusNotFound:
		io.Copy(io.Discard, resp.Body)
		return nil, errNoMessage
	default:
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("kueue receive-batch returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func (t *kueueTarget) queueAck(ctx context.Context, entry ackEntryBench) error {
	t.ackMu.Lock()
	t.ackBatch = append(t.ackBatch, entry)
	shouldFlush := len(t.ackBatch) >= t.ackFlushSize
	t.ackMu.Unlock()

	if shouldFlush {
		// flush synchronously only if batch is full; ticker handles the rest
		return t.flushAcks(ctx)
	}
	return nil
}

func (t *kueueTarget) flushAcks(ctx context.Context) error {
	t.ackMu.Lock()
	if len(t.ackBatch) == 0 {
		t.ackMu.Unlock()
		return nil
	}
	batch := make([]ackEntryBench, len(t.ackBatch))
	copy(batch, t.ackBatch)
	t.ackBatch = t.ackBatch[:0]
	t.ackMu.Unlock()

	type ackBatchReq struct {
		QueueId string          `json:"queueId"`
		Acks    []ackEntryBench `json:"acks"`
	}
	type ackResult struct {
		MessageId     string `json:"messageId"`
		ReceiptHandle string `json:"receiptHandle"`
		Status        string `json:"status"`
		Error         string `json:"error,omitempty"`
	}
	type batchAckResponse struct {
		Results []ackResult `json:"results"`
	}
	var resp batchAckResponse
	if err := t.postJSON(ctx, "/ack", ackBatchReq{QueueId: t.queueID, Acks: batch}, &resp); err != nil {
		return err
	}

	var firstErr error
	for _, r := range resp.Results {
		if r.Status == "ok" {
			continue
		}
		ackID := r.MessageId
		if ackID == "" {
			ackID = r.ReceiptHandle
		}
		err := fmt.Errorf("ack %s failed: %s", ackID, r.Error)
		if firstErr == nil {
			firstErr = err
		}
		if debugLog {
			debugf("batch ack error: %v", err)
		}
	}
	return firstErr
}

func (t *kueueTarget) postJSON(ctx context.Context, path string, requestBody any, out any) error {
	body, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("kueue %s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

type kueuePublisher struct {
	target    *kueueTarget
	mu        sync.Mutex
	batch     []kueuePubMsg
	flushSize int
}

type kueuePubMsg struct {
	Body []byte `json:"body"`
}

func (p *kueuePublisher) Publish(ctx context.Context, body []byte) error {
	p.mu.Lock()
	p.batch = append(p.batch, kueuePubMsg{Body: body})
	shouldFlush := len(p.batch) >= p.flushSize
	p.mu.Unlock()

	if shouldFlush {
		return p.Flush(ctx)
	}
	return nil
}

func (p *kueuePublisher) Flush(ctx context.Context) error {
	p.mu.Lock()
	if len(p.batch) == 0 {
		p.mu.Unlock()
		return nil
	}
	msgs := make([]kueuePubMsg, len(p.batch))
	copy(msgs, p.batch)
	p.batch = p.batch[:0]
	p.mu.Unlock()

	type publishBatchReq struct {
		Messages []kueuePubMsg `json:"messages"`
		QueueId  string        `json:"queueId"`
	}
	return p.target.postJSON(ctx, "/publish-batch", publishBatchReq{
		Messages: msgs,
		QueueId:  p.target.queueID,
	}, nil)
}

func (p *kueuePublisher) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return p.Flush(ctx)
}

func (t *kueueTarget) receive(ctx context.Context, wait bool) (*delivery, error) {
	url := fmt.Sprintf("%s/receive?id=%s", t.baseURL, t.queueID)
	if wait {
		url += "&wait=true"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := t.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var payload struct {
			ID            string `json:"id"`
			Body          []byte `json:"body"`
			DeliveryToken string `json:"deliveryToken"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, err
		}
		return &delivery{
			ID:   payload.ID,
			Body: payload.Body,
			Ack: func(ctx context.Context) error {
				type ackRequest struct {
					MessageID     string `json:"messageId"`
					QueueID       string `json:"queueId"`
					DeliveryToken string `json:"deliveryToken"`
				}
				return t.postJSON(ctx, "/ack", ackRequest{
					MessageID:     payload.ID,
					QueueID:       t.queueID,
					DeliveryToken: payload.DeliveryToken,
				}, nil)
			},
		}, nil
	case http.StatusNotFound:
		io.Copy(io.Discard, resp.Body)
		return nil, errNoMessage
	default:
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("kueue receive returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

type rabbitMQTarget struct {
	uri           string
	queueName     string
	prefetch      int
	consumerCount int
	conn          *amqp.Connection
	adminChannel  *amqp.Channel
	consumerChans []*amqp.Channel
	deliveries    chan amqp.Delivery
}

func newRabbitMQTarget(uri string) *rabbitMQTarget {
	return &rabbitMQTarget{uri: uri}
}

func (t *rabbitMQTarget) Name() string {
	return "rabbitmq"
}

func (t *rabbitMQTarget) Setup(ctx context.Context, cfg workloadConfig, runLabel string) error {
	conn, err := amqp.Dial(t.uri)
	if err != nil {
		return err
	}

	adminChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	queue, err := adminChannel.QueueDeclare(
		runLabel,
		true,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		adminChannel.Close()
		conn.Close()
		return err
	}

	t.queueName = queue.Name
	t.prefetch = cfg.Prefetch
	t.consumerCount = cfg.Consumers
	t.conn = conn
	t.adminChannel = adminChannel
	t.deliveries = make(chan amqp.Delivery, cfg.Prefetch*cfg.Consumers)
	out := t.deliveries

	for i := 0; i < cfg.Consumers; i++ {
		ch, err := conn.Channel()
		if err != nil {
			t.Cleanup(ctx)
			return err
		}

		if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
			ch.Close()
			t.Cleanup(ctx)
			return err
		}

		deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
		if err != nil {
			ch.Close()
			t.Cleanup(ctx)
			return err
		}

		t.consumerChans = append(t.consumerChans, ch)
		go func(deliveries <-chan amqp.Delivery) {
			for delivery := range deliveries {
				out <- delivery
			}
		}(deliveries)
	}

	return nil
}

func (t *rabbitMQTarget) NewPublisher(int) (publisher, error) {
	ch, err := t.conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, err
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	return &rabbitMQPublisher{
		channel:   ch,
		confirms:  confirms,
		queueName: t.queueName,
	}, nil
}

func (t *rabbitMQTarget) Consume(ctx context.Context) (*delivery, error) {
	select {
	case message, ok := <-t.deliveries:
		if !ok {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, io.EOF
		}
		return &delivery{
			ID:   message.MessageId,
			Body: message.Body,
			Ack: func(context.Context) error {
				return message.Ack(false)
			},
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *rabbitMQTarget) Cleanup(context.Context) error {
	var firstErr error

	if t.adminChannel != nil && t.queueName != "" {
		if _, err := t.adminChannel.QueueDelete(t.queueName, false, false, false); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, ch := range t.consumerChans {
		if err := ch.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if t.adminChannel != nil {
		if err := t.adminChannel.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if t.conn != nil {
		if err := t.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	t.queueName = ""
	t.prefetch = 0
	t.consumerCount = 0
	t.conn = nil
	t.adminChannel = nil
	t.consumerChans = nil
	t.deliveries = nil

	return firstErr
}

type rabbitMQPublisher struct {
	channel   *amqp.Channel
	confirms  <-chan amqp.Confirmation
	queueName string
}

func (p *rabbitMQPublisher) Publish(ctx context.Context, body []byte) error {
	if err := p.channel.PublishWithContext(ctx, "", p.queueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Timestamp:    time.Now(),
	}); err != nil {
		return err
	}

	select {
	case confirm, ok := <-p.confirms:
		if !ok {
			return errors.New("rabbitmq publisher confirm channel closed")
		}
		if !confirm.Ack {
			return errors.New("rabbitmq publisher received nack")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *rabbitMQPublisher) Close() error {
	return p.channel.Close()
}
