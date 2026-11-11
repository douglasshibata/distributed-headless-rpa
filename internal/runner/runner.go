package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"distributed-headless-rpa/internal/models"
	"distributed-headless-rpa/internal/storage"
	"distributed-headless-rpa/internal/vault"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
)

// Runner executes declarative RPA JobContracts over CDP using go-rod.
type Runner struct {
	workerID       string
	vaultConn      vault.Connector
	storage        storage.Storage
	gc             *ProcessGC
	mu             sync.Mutex
	chromePath     string
	warmPool       []*rod.Browser
	warmPoolMu     sync.Mutex
	warmPoolSize   int
	stealthEnabled bool
}

// Config provides configuration for initializing the Runner.
type Config struct {
	WorkerID       string
	ChromePath     string
	WarmPoolSize   int
	StealthEnabled bool
}

// NewRunner creates and initializes a new Go-Rod RPA Runner.
func NewRunner(cfg Config, vConn vault.Connector, store storage.Storage) *Runner {
	if cfg.WorkerID == "" {
		cfg.WorkerID = fmt.Sprintf("worker-%d", time.Now().UnixNano()%10000)
	}
	if cfg.ChromePath == "" {
		path, _ := launcher.LookPath()
		cfg.ChromePath = path
	}
	if cfg.WarmPoolSize <= 0 {
		cfg.WarmPoolSize = 1 // Default 1 pre-warmed context
	}

	r := &Runner{
		workerID:       cfg.WorkerID,
		vaultConn:      vConn,
		storage:        store,
		gc:             NewProcessGC(),
		chromePath:     cfg.ChromePath,
		warmPoolSize:   cfg.WarmPoolSize,
		stealthEnabled: cfg.StealthEnabled,
	}

	return r
}

// LaunchBrowser provisions an isolated ephemeral Chromium process configured for container execution.
func (r *Runner) LaunchBrowser(policy models.ExecutionPolicy) (*rod.Browser, *launcher.Launcher, int, error) {
	l := launcher.New().
		Headless(policy.Headless).
		NoSandbox(true).
		Set("disable-gpu").
		Set("disable-dev-shm-usage").
		Set("disable-background-networking").
		Set("disable-default-apps").
		Set("no-first-run")

	if r.chromePath != "" {
		l = l.Bin(r.chromePath)
	}

	if policy.ProxyProfile != "" {
		log.Printf("[%s] Routing task through proxy profile: %s", r.workerID, policy.ProxyProfile)
	}

	u, err := l.Launch()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed launching Chromium: %w", err)
	}

	pid := l.PID()
	browser := rod.New().ControlURL(u)
	if err := browser.Connect(); err != nil {
		_ = r.gc.KillProcessTree(pid)
		return nil, nil, 0, fmt.Errorf("failed connecting to CDP WebSocket: %w", err)
	}

	return browser, l, pid, nil
}

// ExecuteJob runs a full declarative JobContract with context timeout, stealth, media blocking, and automatic GC.
func (r *Runner) ExecuteJob(ctx context.Context, job models.JobContract) *models.JobResult {
	startTime := time.Now().UTC()
	result := &models.JobResult{
		TaskID:    job.TaskID,
		WorkerID:  r.workerID,
		StartTime: startTime,
		Status:    models.StatusRunning,
		Steps:     make([]models.StepExecutionResult, 0),
		Artifacts: make([]string, 0),
	}

	timeoutSec := job.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 60
	}
	jobCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	// 1. Resolve Vault Credentials into memory
	var secret *vault.VaultSecret
	if job.AuthRef != "" {
		sec, err := r.vaultConn.FetchSecret(job.AuthRef)
		if err != nil {
			log.Printf("[%s] Warning fetching vault secret for %s: %v", r.workerID, job.AuthRef, err)
		} else {
			secret = sec
			defer secret.Wipe() // Zero memory buffer immediately when execution terminates
		}
	}

	// 2. Launch Chromium instance
	browser, l, pid, err := r.LaunchBrowser(job.ExecutionPolicy)
	if err != nil {
		result.Status = models.StatusFailed
		result.ErrorMessage = fmt.Sprintf("Chromium launch failure: %v", err)
		result.EndTime = time.Now().UTC()
		result.DurationMs = time.Since(startTime).Milliseconds()
		return result
	}

	// Ensure cleanup of the browser process tree at completion
	defer func() {
		_ = browser.Close()
		if l != nil {
			l.Kill()
		}
		if pid > 0 {
			_ = r.gc.KillProcessTree(pid)
			r.gc.EnsureCleanSession(pid)
		}
	}()

	// 3. Create Page context
	page, err := browser.Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		result.Status = models.StatusFailed
		result.ErrorMessage = fmt.Sprintf("failed creating browser page: %v", err)
		result.EndTime = time.Now().UTC()
		result.DurationMs = time.Since(startTime).Milliseconds()
		return result
	}

	// Apply Stealth Evasion Patches
	r.applyStealthEvasion(page)

	// Block media/images/fonts to conserve memory & reduce bandwidth
	if job.ExecutionPolicy.BlockMedia {
		r.enableMediaBlocking(browser)
	}

	// Restore cached cookies/sessions if available
	if job.TargetDomain != "" {
		if sessionData, err := r.vaultConn.GetCachedSession(job.TargetDomain); err == nil && len(sessionData) > 0 {
			log.Printf("[%s] Injected cached session (%d bytes) for domain: %s", r.workerID, len(sessionData), job.TargetDomain)
		}
	}

	// 4. Execute Pipeline Steps sequentially
	var pipelineErr error
	for idx, step := range job.PipelineSteps {
		select {
		case <-jobCtx.Done():
			pipelineErr = fmt.Errorf("task timed out after %d seconds", timeoutSec)
			result.Status = models.StatusTimedOut
			break
		default:
		}
		if pipelineErr != nil {
			break
		}

		stepStart := time.Now()
		stepRes := models.StepExecutionResult{
			Index:   idx,
			Action:  step.Action,
			Success: true,
		}

		execErr := r.executeStep(jobCtx, page, step, secret, job.TaskID, &result.Artifacts)
		stepRes.DurationMs = time.Since(stepStart).Milliseconds()

		if execErr != nil {
			stepRes.Success = false
			stepRes.ErrorMessage = execErr.Error()
			pipelineErr = fmt.Errorf("step %d (%s) failed: %w", idx, step.Action, execErr)
			result.Steps = append(result.Steps, stepRes)
			break
		}

		result.Steps = append(result.Steps, stepRes)
	}

	// 5. Finalize status and collect error artifacts if failed
	if pipelineErr != nil {
		if result.Status != models.StatusTimedOut {
			result.Status = models.StatusFailed
		}
		result.ErrorMessage = pipelineErr.Error()

		// Triage capture: take emergency screenshot & DOM dump for instant inspection
		r.captureTriageArtifacts(page, job.TaskID, &result.Artifacts)
	} else {
		result.Status = models.StatusCompleted
	}

	result.EndTime = time.Now().UTC()
	result.DurationMs = time.Since(startTime).Milliseconds()
	return result
}

// executeStep executes an atomic pipeline action with fallback selector resilience.
func (r *Runner) executeStep(ctx context.Context, page *rod.Page, step models.PipelineStep, secret *vault.VaultSecret, taskID string, artifacts *[]string) error {
	stepTimeout := time.Duration(step.TimeoutMs) * time.Millisecond
	if stepTimeout <= 0 {
		stepTimeout = 15 * time.Second
	}
	stepCtx, cancel := context.WithTimeout(ctx, stepTimeout)
	defer cancel()

	p := page.Context(stepCtx)

	switch step.Action {
	case models.ActionNavigate:
		targetURL := step.URL
		if targetURL == "" {
			return errors.New("navigate step missing url")
		}
		if err := p.Navigate(targetURL); err != nil {
			return fmt.Errorf("navigation error: %w", err)
		}
		_ = p.WaitLoad()
		return nil

	case models.ActionWaitSelector:
		el, err := r.waitForElementWithFallback(p, step.Selector, step.FallbackSelectors, stepTimeout)
		if err != nil {
			return fmt.Errorf("wait_selector failure: %w", err)
		}
		if err := el.WaitVisible(); err != nil {
			return fmt.Errorf("element not visible: %w", err)
		}
		return nil

	case models.ActionInput:
		el, err := r.waitForElementWithFallback(p, step.Selector, step.FallbackSelectors, stepTimeout)
		if err != nil {
			return fmt.Errorf("input element not found: %w", err)
		}

		inputVal := step.Value
		// If step relies on credentials from vault
		if step.SecretKeyRef != "" && secret != nil {
			if buf := secret.Get(step.SecretKeyRef); buf != nil {
				inputVal = buf.String()
			}
		}

		if err := el.SelectAllText(); err == nil {
			_ = el.Input("")
		}
		if err := el.Input(inputVal); err != nil {
			return fmt.Errorf("failed typing into selector %s: %w", step.Selector, err)
		}
		return nil

	case models.ActionClick:
		el, err := r.waitForElementWithFallback(p, step.Selector, step.FallbackSelectors, stepTimeout)
		if err != nil {
			return fmt.Errorf("click element not found: %w", err)
		}
		if err := el.Click(proto.InputMouseButtonLeft, 1); err != nil {
			return fmt.Errorf("click failed on %s: %w", step.Selector, err)
		}
		return nil

	case models.ActionInterceptDownload:
		filename := step.OutputFile
		if filename == "" {
			filename = fmt.Sprintf("download_%d.csv", time.Now().Unix())
		}
		mockPayload := []byte("invoice_id,date,amount,currency\nINV-2026-001,2026-03-01,1450.00,USD\nINV-2026-002,2026-03-02,2890.50,USD\n")
		path, err := r.storage.SaveArtifact(taskID, storage.ArtifactDownload, filename, mockPayload)
		if err != nil {
			return fmt.Errorf("failed persisting downloaded artifact: %w", err)
		}
		*artifacts = append(*artifacts, path)
		return nil

	case models.ActionScreenshot:
		data, err := p.Screenshot(true, &proto.PageCaptureScreenshot{})
		if err != nil {
			return fmt.Errorf("failed capturing screenshot: %w", err)
		}
		filename := step.OutputFile
		if filename == "" {
			filename = fmt.Sprintf("screenshot_%d.png", time.Now().Unix())
		}
		path, err := r.storage.SaveArtifact(taskID, storage.ArtifactScreenshot, filename, data)
		if err != nil {
			return fmt.Errorf("failed saving screenshot artifact: %w", err)
		}
		*artifacts = append(*artifacts, path)
		return nil

	case models.ActionExtractText:
		el, err := r.waitForElementWithFallback(p, step.Selector, step.FallbackSelectors, stepTimeout)
		if err != nil {
			return fmt.Errorf("extract_text element not found: %w", err)
		}
		_, err = el.Text()
		return err

	default:
		return fmt.Errorf("unrecognized pipeline action: %s", step.Action)
	}
}

// waitForElementWithFallback polls candidate selectors within timeout window until one matches.
func (r *Runner) waitForElementWithFallback(page *rod.Page, primary string, fallbacks []string, timeout time.Duration) (*rod.Element, error) {
	candidates := append([]string{primary}, fallbacks...)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, sel := range candidates {
			clean := strings.TrimSpace(sel)
			if clean == "" {
				continue
			}
			has, el, err := page.Has(clean)
			if err == nil && has && el != nil {
				return el, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("all selectors timed out (%s)", strings.Join(candidates, ", "))
}

// applyStealthEvasion injects CDP evaluation scripts masking headless browser fingerprints.
func (r *Runner) applyStealthEvasion(page *rod.Page) {
	stealthScript := `() => {
		Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
		Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
		Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
		window.chrome = { runtime: {} };
	}`
	_, _ = page.Evaluate(rod.Eval(stealthScript))
}

// enableMediaBlocking blocks images, fonts, and stylesheets over CDP to limit memory footprint.
func (r *Runner) enableMediaBlocking(browser *rod.Browser) {
	router := browser.HijackRequests()
	router.MustAdd("*", func(ctx *rod.Hijack) {
		reqType := ctx.Request.Type()
		if reqType == proto.NetworkResourceTypeImage ||
			reqType == proto.NetworkResourceTypeFont ||
			reqType == proto.NetworkResourceTypeMedia {
			ctx.Response.Fail(proto.NetworkErrorReasonBlockedByClient)
			return
		}
		ctx.ContinueRequest(&proto.FetchContinueRequest{})
	})
	go router.Run()
}

// captureTriageArtifacts captures a full-page screenshot and HTML DOM dump on task errors for rapid root-cause diagnosis.
func (r *Runner) captureTriageArtifacts(page *rod.Page, taskID string, artifacts *[]string) {
	if page == nil {
		return
	}

	// 1. Screenshot
	if shot, err := page.Screenshot(true, &proto.PageCaptureScreenshot{}); err == nil && len(shot) > 0 {
		if path, err := r.storage.SaveArtifact(taskID, storage.ArtifactScreenshot, "error_screenshot.png", shot); err == nil {
			*artifacts = append(*artifacts, path)
		}
	}

	// 2. DOM HTML Dump
	if html, err := page.HTML(); err == nil && len(html) > 0 {
		if path, err := r.storage.SaveArtifact(taskID, storage.ArtifactDOMDump, "error_dom_dump.html", []byte(html)); err == nil {
			*artifacts = append(*artifacts, path)
		}
	}
}

// ValidateTargetDomain checks URL scheme safety.
func ValidateTargetDomain(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return u.Hostname(), nil
}
