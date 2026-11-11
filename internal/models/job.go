package models

import "time"

// TaskStatus represents the current state of an RPA job.
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"
	StatusRunning   TaskStatus = "RUNNING"
	StatusCompleted TaskStatus = "COMPLETED"
	StatusFailed    TaskStatus = "FAILED"
	StatusTimedOut  TaskStatus = "TIMED_OUT"
)

// ExecutionPolicy configures browser and runtime behavior for a task.
type ExecutionPolicy struct {
	Headless     bool   `json:"headless"`
	BlockMedia   bool   `json:"block_media"`
	ProxyProfile string `json:"proxy_profile,omitempty"`
	Retries      int    `json:"retries"`
}

// StepAction represents supported pipeline automation actions.
type StepAction string

const (
	ActionNavigate          StepAction = "navigate"
	ActionWaitSelector      StepAction = "wait_selector"
	ActionInput             StepAction = "input"
	ActionClick             StepAction = "click"
	ActionInterceptDownload StepAction = "intercept_download"
	ActionScreenshot        StepAction = "screenshot"
	ActionExtractText       StepAction = "extract_text"
)

// PipelineStep defines an atomic browser interaction instruction.
type PipelineStep struct {
	Action            StepAction `json:"action"`
	URL               string     `json:"url,omitempty"`
	Selector          string     `json:"selector,omitempty"`
	FallbackSelectors []string   `json:"fallback_selectors,omitempty"`
	Value             string     `json:"value,omitempty"`
	SecretKeyRef      string     `json:"secret_key_ref,omitempty"` // Key inside auth_ref payload
	TimeoutMs         int        `json:"timeout_ms,omitempty"`
	DestinationBucket string     `json:"destination_bucket,omitempty"`
	OutputFile        string     `json:"output_file,omitempty"`
}

// JobContract is the declarative job definition submitted to the RPA fleet.
type JobContract struct {
	TaskID          string          `json:"task_id"`
	TargetDomain    string          `json:"target_domain"`
	Entrypoint      string          `json:"entrypoint"`
	AuthRef         string          `json:"auth_ref,omitempty"`
	TimeoutSeconds  int             `json:"timeout_seconds"`
	ExecutionPolicy ExecutionPolicy `json:"execution_policy"`
	PipelineSteps   []PipelineStep  `json:"pipeline_steps"`
	CallbackWebhook string          `json:"callback_webhook,omitempty"`
}

// StepExecutionResult tracks the outcome of an individual pipeline step.
type StepExecutionResult struct {
	Index        int        `json:"index"`
	Action       StepAction `json:"action"`
	Success      bool       `json:"success"`
	ErrorMessage string     `json:"error_message,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	ArtifactURL  string     `json:"artifact_url,omitempty"`
	ExtractedVal string     `json:"extracted_val,omitempty"`
}

// JobResult details the overall execution output and artifacts.
type JobResult struct {
	TaskID       string                `json:"task_id"`
	Status       TaskStatus            `json:"status"`
	WorkerID     string                `json:"worker_id"`
	StartTime    time.Time             `json:"start_time"`
	EndTime      time.Time             `json:"end_time"`
	DurationMs   int64                 `json:"duration_ms"`
	Steps        []StepExecutionResult `json:"steps"`
	Artifacts    []string              `json:"artifacts"`
	ErrorMessage string                `json:"error_message,omitempty"`
}

// CallbackPayload is dispatched to the caller webhook upon completion.
type CallbackPayload struct {
	TaskID       string                `json:"task_id"`
	Status       TaskStatus            `json:"status"`
	DurationMs   int64                 `json:"duration_ms"`
	Artifacts    []string              `json:"artifacts"`
	ErrorMessage string                `json:"error_message,omitempty"`
	StepsSummary []StepExecutionResult `json:"steps_summary"`
	CompletedAt  time.Time             `json:"completed_at"`
}
