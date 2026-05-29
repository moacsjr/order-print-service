package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alexbrainman/printer"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"gopkg.in/yaml.v3"
)

// ---------- Configuration ----------

type Config struct {
	AWS     AWSConfig     `yaml:"aws"`
	Printer PrinterConfig `yaml:"printer"`
	Worker  WorkerConfig  `yaml:"worker"`
	Logging LoggingConfig `yaml:"logging"`
}

type AWSConfig struct {
	Region   string `yaml:"region"`
	QueueURL string `yaml:"queue_url"`
	DLQURL   string `yaml:"dlq_url"`
	Profile  string `yaml:"profile"`
}

type PrinterConfig struct {
	Name string `yaml:"name"`
	Mode string `yaml:"mode"` // "RAW" or "TEXT"
}

type WorkerConfig struct {
	MaxNumberOfMessages int32 `yaml:"max_number_of_messages"`
	WaitTimeSeconds     int32 `yaml:"wait_time_seconds"`
	BackoffSeconds      int   `yaml:"backoff_seconds"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

// ---------- Global state ----------

var (
	cfg       Config
	sqsClient *sqs.Client
	printerW  *printer.Printer
	logLevel  int
)

const (
	LOG_DEBUG = 0
	LOG_INFO  = 1
	LOG_ERROR = 2
)

func logf(level int, format string, args ...interface{}) {
	if level >= logLevel {
		prefix := "[INFO]"
		if level == LOG_DEBUG {
			prefix = "[DEBUG]"
		} else if level == LOG_ERROR {
			prefix = "[ERROR]"
		}
		log.Printf(prefix+" "+format, args...)
	}
}

// ---------- Initialization ----------

func main() {
	if err := loadConfig(); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := initSQS(ctx); err != nil {
		log.Fatalf("Failed to initialize SQS client: %v", err)
	}

	if err := initPrinter(); err != nil {
		log.Fatalf("Failed to initialize printer: %v", err)
	}
	defer printerW.Close()

	// Start HTTP dashboard
	go startDashboard(ctx)

	// Graceful shutdown: intercept OS signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logf(LOG_INFO, "Received signal %v, shutting down…", sig)
		cancel()
	}()

	// Drain DLQ on startup before processing the main queue
	logf(LOG_INFO, "Draining DLQ before starting main worker loop…")
	drainDLQ(ctx)

	// Main worker loop
	runWorker(ctx)

	logf(LOG_INFO, "Worker stopped.")
}

func loadConfig() error {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		return fmt.Errorf("reading config.yaml: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}

	// Apply hard limits from spec
	if cfg.Worker.MaxNumberOfMessages > 10 {
		cfg.Worker.MaxNumberOfMessages = 10
	}
	if cfg.Worker.WaitTimeSeconds > 20 {
		cfg.Worker.WaitTimeSeconds = 20
	}

	// Set log level
	switch strings.ToUpper(cfg.Logging.Level) {
	case "DEBUG":
		logLevel = LOG_DEBUG
	case "ERROR":
		logLevel = LOG_ERROR
	default:
		logLevel = LOG_INFO
	}

	logf(LOG_INFO, "Config loaded: region=%s queue=%s printer=%q mode=%s",
		cfg.AWS.Region, cfg.AWS.QueueURL, cfg.Printer.Name, cfg.Printer.Mode)
	return nil
}

func initSQS(ctx context.Context) error {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AWS.Region),
	}
	if cfg.AWS.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(cfg.AWS.Profile))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	sqsClient = sqs.NewFromConfig(awsCfg)
	return nil
}

func initPrinter() error {
	var p *printer.Printer
	var err error

	if cfg.Printer.Name != "" {
		p, err = printer.Open(cfg.Printer.Name)
		if err != nil {
			return fmt.Errorf("opening printer %q: %w", cfg.Printer.Name, err)
		}
	} else {
		name, err := printer.Default()
		if err != nil {
			return fmt.Errorf("getting default printer name: %w", err)
		}
		p, err = printer.Open(name)
		if err != nil {
			return fmt.Errorf("opening default printer %q: %w", name, err)
		}
		cfg.Printer.Name = name
		logf(LOG_INFO, "Using default printer: %s", name)
	}
	printerW = p
	return nil
}

// ---------- Printer logic ----------

func printText(body string) error {
	mode := "TEXT"
	if strings.ToUpper(cfg.Printer.Mode) == "RAW" {
		mode = "RAW"
	}

	if err := printerW.StartDocument("Order Print Job", mode); err != nil {
		return fmt.Errorf("failed to start document: %w", err)
	}
	defer printerW.EndDocument()

	if err := printerW.StartPage(); err != nil {
		return fmt.Errorf("failed to start page: %w", err)
	}
	defer printerW.EndPage()

	_, err := printerW.Write([]byte(body))
	if err != nil {
		return fmt.Errorf("failed writing to print pool: %w", err)
	}

	return nil
}

// ---------- SQS Worker ----------

func runWorker(ctx context.Context) {
	logf(LOG_INFO, "Starting worker loop: queue=%s max_msgs=%d wait=%ds",
		cfg.AWS.QueueURL, cfg.Worker.MaxNumberOfMessages, cfg.Worker.WaitTimeSeconds)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resp, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &cfg.AWS.QueueURL,
			MaxNumberOfMessages: cfg.Worker.MaxNumberOfMessages,
			WaitTimeSeconds:     cfg.Worker.WaitTimeSeconds,
		})

		if err != nil {
			logf(LOG_ERROR, "ReceiveMessage error: %v (backing off %ds)", err, cfg.Worker.BackoffSeconds)
			sleepCtx(ctx, time.Duration(cfg.Worker.BackoffSeconds)*time.Second)
			continue
		}

		for _, msg := range resp.Messages {
			if ctx.Err() != nil {
				return
			}

			if err := printText(*msg.Body); err != nil {
				logf(LOG_ERROR, "Print failed for message %s — routing to DLQ: %v", *msg.MessageId, err)

				if _, sendErr := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
					QueueUrl:               &cfg.AWS.DLQURL,
					MessageBody:            msg.Body,
					MessageDeduplicationId: msg.MessageId,
				}); sendErr != nil {
					logf(LOG_ERROR, "Failed to send message %s to DLQ: %v", *msg.MessageId, sendErr)
				} else {
					logf(LOG_INFO, "Message %s sent to DLQ", *msg.MessageId)
				}

				deleteFromMainQueue(ctx, &msg)
			} else {
				logf(LOG_INFO, "Successfully printed message %s", *msg.MessageId)
				deleteFromMainQueue(ctx, &msg)
			}
		}
	}
}

func deleteFromMainQueue(ctx context.Context, msg *types.Message) {
	if _, err := sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &cfg.AWS.QueueURL,
		ReceiptHandle: msg.ReceiptHandle,
	}); err != nil {
		logf(LOG_ERROR, "Failed to delete message %s from main queue: %v", *msg.MessageId, err)
	}
}

// ---------- DLQ Drain ----------

func drainDLQ(ctx context.Context) {
	logf(LOG_INFO, "Starting DLQ drain from %s", cfg.AWS.DLQURL)
	processed := 0

	for {
		if ctx.Err() != nil {
			logf(LOG_INFO, "DLQ drain cancelled")
			return
		}

		resp, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &cfg.AWS.DLQURL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     5,
		})
		if err != nil {
			logf(LOG_ERROR, "Error reading from DLQ: %v", err)
			return
		}
		if len(resp.Messages) == 0 {
			break
		}

		for _, msg := range resp.Messages {
			if err := printText(*msg.Body); err != nil {
				logf(LOG_ERROR, "DLQ print failed for message %s — aborting drain: %v", *msg.MessageId, err)
				return
			}

			if _, err := sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &cfg.AWS.DLQURL,
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				logf(LOG_ERROR, "Failed to delete message %s from DLQ: %v", *msg.MessageId, err)
				return
			}
			processed++
			logf(LOG_INFO, "DLQ message %s printed and deleted", *msg.MessageId)
		}
	}

	logf(LOG_INFO, "DLQ drain complete. %d messages processed.", processed)
}

// ---------- HTTP Dashboard ----------

type dashboardHandler struct {
	drainMu sync.Mutex
	drainCh chan struct{}
	drainWg sync.WaitGroup
}

var dashHandler = &dashboardHandler{
	drainCh: make(chan struct{}, 1),
}

func startDashboard(ctx context.Context) {
	mux := http.NewServeMux()

	mux.Handle("/", http.FileServer(http.Dir(".")))
	mux.HandleFunc("/api/test-print", handleTestPrint)
	mux.HandleFunc("/api/dlq", handleDLQPeek)
	mux.HandleFunc("/api/dlq/reprocess", handleDLQReprocess)

	srv := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	logf(LOG_INFO, "Dashboard listening on http://localhost:8080")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logf(LOG_ERROR, "Dashboard server error: %v", err)
	}
}

func handleTestPrint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	payload := fmt.Sprintf(
		"=== TEST PRINT ===\nTimestamp: %s\nSource: Dashboard\nStatus: OK\n==================\n\n",
		time.Now().Format("2006-01-02 15:04:05"),
	)

	err := printText(payload)
	if err != nil {
		jsonResp(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	jsonResp(w, http.StatusOK, map[string]string{
		"status": "ok",
		"msg":    "Test print sent to " + cfg.Printer.Name,
	})
}

func handleDLQPeek(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}

	resp, err := sqsClient.ReceiveMessage(r.Context(), &sqs.ReceiveMessageInput{
		QueueUrl:            &cfg.AWS.DLQURL,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   2,
	})
	if err != nil {
		jsonResp(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	for _, msg := range resp.Messages {
		sqsClient.ChangeMessageVisibility(r.Context(), &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          &cfg.AWS.DLQURL,
			ReceiptHandle:     msg.ReceiptHandle,
			VisibilityTimeout: 0,
		})
	}

	type dlqItem struct {
		ID   string `json:"id"`
		Body string `json:"body"`
	}

	items := make([]dlqItem, len(resp.Messages))
	for i, msg := range resp.Messages {
		body := *msg.Body
		if len(body) > 120 {
			body = body[:120] + "…"
		}
		items[i] = dlqItem{ID: *msg.MessageId, Body: body}
	}

	jsonResp(w, http.StatusOK, map[string]interface{}{
		"status":   "ok",
		"count":    len(items),
		"messages": items,
	})
}

func handleDLQReprocess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	if !dashHandler.drainMu.TryLock() {
		jsonResp(w, http.StatusConflict, map[string]string{
			"status": "error",
			"error":  "DLQ drain already in progress",
		})
		return
	}

	go func() {
		defer dashHandler.drainMu.Unlock()
		drainDLQ(r.Context())
	}()

	jsonResp(w, http.StatusOK, map[string]string{
		"status": "ok",
		"msg":    "DLQ drain started",
	})
}

// ---------- Helpers ----------

func jsonResp(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
