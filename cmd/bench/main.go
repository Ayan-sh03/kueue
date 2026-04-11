package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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

type workloadConfig struct {
	Messages     int      `json:"messages"`
	Warmup       int      `json:"warmup"`
	Runs         int      `json:"runs"`
	PayloadBytes int      `json:"payloadBytes"`
	Producers    int      `json:"producers"`
	Consumers    int      `json:"consumers"`
	Prefetch     int      `json:"prefetch"`
	Targets      []string `json:"targets"`
}

type benchmarkPayload struct {
	Seq            int    `json:"seq"`
	SentAtUnixNano int64  `json:"sentAtUnixNano"`
	Padding        string `json:"padding,omitempty"`
}

type benchmarkResult struct {
	Target            string  `json:"target"`
	Run               int     `json:"run"`
	Messages          int     `json:"messages"`
	PayloadBytes      int     `json:"payloadBytes"`
	Producers         int     `json:"producers"`
	Consumers         int     `json:"consumers"`
	PublishSeconds    float64 `json:"publishSeconds"`
	ConsumeSeconds    float64 `json:"consumeSeconds"`
	EndToEndSeconds   float64 `json:"endToEndSeconds"`
	PublishRate       float64 `json:"publishRate"`
	ConsumeRate       float64 `json:"consumeRate"`
	LatencyP50Ms      float64 `json:"latencyP50Ms"`
	LatencyP95Ms      float64 `json:"latencyP95Ms"`
	LatencyP99Ms      float64 `json:"latencyP99Ms"`
	LatencyMaxMs      float64 `json:"latencyMaxMs"`
	PublishedMessages int64   `json:"publishedMessages"`
	ConsumedMessages  int64   `json:"consumedMessages"`
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
		targets      = flag.String("targets", "kueue,rabbitmq", "comma-separated benchmark targets")
		kueueURL     = flag.String("kueue-url", "http://127.0.0.1:8080", "base URL for the kueue HTTP server")
		rabbitMQURI  = flag.String("rabbitmq-uri", "amqp://guest:guest@127.0.0.1:5672/", "AMQP URI for RabbitMQ")
		messages     = flag.Int("messages", 10000, "messages per measured run")
		warmup       = flag.Int("warmup", 500, "warmup messages before each measured run")
		runs         = flag.Int("runs", 3, "measured runs per target")
		payloadBytes = flag.Int("payload-bytes", 256, "message body size in bytes")
		producers    = flag.Int("producers", 1, "concurrent publishers")
		consumers    = flag.Int("consumers", 1, "concurrent consumers")
		prefetch     = flag.Int("prefetch", 200, "RabbitMQ consumer prefetch per consumer")
		jsonOut      = flag.String("json-out", "", "optional path for benchmark report JSON")
	)
	flag.Parse()

	cfg := workloadConfig{
		Messages:     *messages,
		Warmup:       *warmup,
		Runs:         *runs,
		PayloadBytes: *payloadBytes,
		Producers:    *producers,
		Consumers:    *consumers,
		Prefetch:     *prefetch,
		Targets:      parseTargets(*targets),
	}

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}

	report := benchmarkReport{
		GeneratedAt: time.Now().UTC(),
		Workload:    cfg,
	}

	for _, name := range cfg.Targets {
		target, err := newTarget(name, *kueueURL, *rabbitMQURI)
		if err != nil {
			fmt.Fprintf(os.Stderr, "target %q: %v\n", name, err)
			os.Exit(1)
		}

		summary, err := runTarget(context.Background(), target, cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s benchmark failed: %v\n", target.Name(), err)
			os.Exit(1)
		}

		report.Summaries = append(report.Summaries, summary)
	}

	printReport(report)

	if *jsonOut != "" {
		if err := writeReport(*jsonOut, report); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(1)
		}
	}
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
		runLabel := fmt.Sprintf("%s-%d-%d", target.Name(), run, time.Now().UnixNano())
		if err := target.Setup(ctx, cfg, runLabel); err != nil {
			return summary, err
		}

		if cfg.Warmup > 0 {
			if _, err := executeWorkload(ctx, target, cfg, cfg.Warmup); err != nil {
				_ = target.Cleanup(context.Background())
				return summary, fmt.Errorf("warmup run %d: %w", run, err)
			}
		}

		result, err := executeWorkload(ctx, target, cfg, cfg.Messages)
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
			"run %d: publish %.0f msg/s, consume %.0f msg/s, latency p50 %.2f ms, p95 %.2f ms, p99 %.2f ms\n",
			run,
			result.PublishRate,
			result.ConsumeRate,
			result.LatencyP50Ms,
			result.LatencyP95Ms,
			result.LatencyP99Ms,
		)
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
		latencyMu sync.Mutex
		latencies []time.Duration
		seqCh     = make(chan int, messages)
		produced  atomic.Int64
		consumed  atomic.Int64
		firstErr  error
		errOnce   sync.Once
	)

	recordErr := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancelRun()
		})
	}

	for seq := 0; seq < messages; seq++ {
		seqCh <- seq
	}
	close(seqCh)

	consumeStarted := time.Now()
	var consumerWG sync.WaitGroup
	for i := 0; i < cfg.Consumers; i++ {
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			for {
				if consumed.Load() >= int64(messages) {
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

				if err := msg.Ack(runCtx); err != nil {
					recordErr(err)
					return
				}

				latencyMu.Lock()
				latencies = append(latencies, latency)
				latencyMu.Unlock()

				if consumed.Add(1) >= int64(messages) {
					cancelConsume()
					return
				}
			}
		}()
	}

	publishStarted := time.Now()
	var producerWG sync.WaitGroup
	for i := 0; i < cfg.Producers; i++ {
		pub, err := target.NewPublisher(i)
		if err != nil {
			cancelRun()
			consumerWG.Wait()
			return result, err
		}

		producerWG.Add(1)
		go func(p publisher) {
			defer producerWG.Done()
			defer p.Close()

			for seq := range seqCh {
				body, err := makePayload(seq, cfg.PayloadBytes)
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
		}(pub)
	}

	producerWG.Wait()
	publishDone := time.Now()

	if firstErr != nil {
		consumerWG.Wait()
		return result, firstErr
	}

	consumerWG.Wait()
	consumeDone := time.Now()

	if firstErr != nil {
		return result, firstErr
	}
	if produced.Load() != int64(messages) {
		return result, fmt.Errorf("published %d/%d messages", produced.Load(), messages)
	}
	if consumed.Load() != int64(messages) {
		return result, fmt.Errorf("consumed %d/%d messages", consumed.Load(), messages)
	}
	if len(latencies) != messages {
		return result, fmt.Errorf("recorded %d/%d latencies", len(latencies), messages)
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	result.PublishSeconds = publishDone.Sub(publishStarted).Seconds()
	result.ConsumeSeconds = consumeDone.Sub(consumeStarted).Seconds()
	result.EndToEndSeconds = consumeDone.Sub(publishStarted).Seconds()
	result.PublishRate = float64(messages) / result.PublishSeconds
	result.ConsumeRate = float64(messages) / result.ConsumeSeconds
	result.LatencyP50Ms = percentileMs(latencies, 0.50)
	result.LatencyP95Ms = percentileMs(latencies, 0.95)
	result.LatencyP99Ms = percentileMs(latencies, 0.99)
	result.LatencyMaxMs = latencyToMs(latencies[len(latencies)-1])
	result.PublishedMessages = produced.Load()
	result.ConsumedMessages = consumed.Load()

	return result, nil
}

func makePayload(seq, payloadBytes int) ([]byte, error) {
	payload := benchmarkPayload{
		Seq:            seq,
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

func medianResult(target string, runs []benchmarkResult) benchmarkResult {
	if len(runs) == 0 {
		return benchmarkResult{Target: target}
	}

	return benchmarkResult{
		Target:            target,
		Run:               0,
		Messages:          runs[0].Messages,
		PayloadBytes:      runs[0].PayloadBytes,
		Producers:         runs[0].Producers,
		Consumers:         runs[0].Consumers,
		PublishSeconds:    medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.PublishSeconds })),
		ConsumeSeconds:    medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.ConsumeSeconds })),
		EndToEndSeconds:   medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.EndToEndSeconds })),
		PublishRate:       medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.PublishRate })),
		ConsumeRate:       medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.ConsumeRate })),
		LatencyP50Ms:      medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyP50Ms })),
		LatencyP95Ms:      medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyP95Ms })),
		LatencyP99Ms:      medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyP99Ms })),
		LatencyMaxMs:      medianFloat(extractMetric(runs, func(r benchmarkResult) float64 { return r.LatencyMaxMs })),
		PublishedMessages: runs[0].PublishedMessages,
		ConsumedMessages:  runs[0].ConsumedMessages,
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
	fmt.Printf("%-10s %-12s %-12s %-12s %-12s %-12s\n", "target", "pub msg/s", "con msg/s", "p50 ms", "p95 ms", "p99 ms")
	for _, summary := range report.Summaries {
		median := summary.MedianResult
		fmt.Printf(
			"%-10s %-12.0f %-12.0f %-12.2f %-12.2f %-12.2f\n",
			summary.Target,
			median.PublishRate,
			median.ConsumeRate,
			median.LatencyP50Ms,
			median.LatencyP95Ms,
			median.LatencyP99Ms,
		)
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

type kueueTarget struct {
	baseURL string
	client  *http.Client
	queueID string
}

func newKueueTarget(baseURL string) *kueueTarget {
	return &kueueTarget{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: 40 * time.Second,
		},
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
	return nil
}

func (t *kueueTarget) NewPublisher(int) (publisher, error) {
	return &kueuePublisher{target: t}, nil
}

func (t *kueueTarget) Consume(ctx context.Context) (*delivery, error) {
	msg, err := t.receive(ctx, false)
	if err == nil {
		return msg, nil
	}
	if !errors.Is(err, errNoMessage) {
		return nil, err
	}
	return t.receive(ctx, true)
}

func (t *kueueTarget) Cleanup(context.Context) error {
	t.queueID = ""
	return nil
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
			ID   string `json:"id"`
			Body []byte `json:"body"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, err
		}
		return &delivery{
			ID:   payload.ID,
			Body: payload.Body,
			Ack: func(ctx context.Context) error {
				type ackRequest struct {
					MessageID string `json:"messageId"`
					QueueID   string `json:"queueId"`
				}
				return t.postJSON(ctx, "/ack", ackRequest{
					MessageID: payload.ID,
					QueueID:   t.queueID,
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
	target *kueueTarget
}

func (p *kueuePublisher) Publish(ctx context.Context, body []byte) error {
	type publishRequest struct {
		Message struct {
			Body []byte `json:"body"`
		} `json:"message"`
		QueueID string `json:"queueId"`
	}

	req := publishRequest{QueueID: p.target.queueID}
	req.Message.Body = body
	return p.target.postJSON(ctx, "/publish", req, nil)
}

func (p *kueuePublisher) Close() error {
	return nil
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
