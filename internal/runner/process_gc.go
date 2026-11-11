package runner

import (
	"fmt"
	"log"
	"os"
	"syscall"
	"time"
)

// ProcessGC manages hard-killing browser process trees and preventing zombie PIDs.
type ProcessGC struct{}

// NewProcessGC creates a process garbage collection manager.
func NewProcessGC() *ProcessGC {
	return &ProcessGC{}
}

// KillProcessTree kills the entire process group associated with a PID using SIGKILL.
// In Linux containers, terminating the parent alone can leave orphaned Chromium zombie processes.
func (gc *ProcessGC) KillProcessTree(pid int) error {
	if pid <= 0 {
		return nil
	}

	log.Printf("[ProcessGC] Initiating hard-kill for process tree with root PID: %d", pid)

	// First try to kill process group (-pid)
	pgid, err := syscall.Getpgid(pid)
	if err == nil && pgid > 0 {
		// Send SIGKILL to the entire process group
		if err := syscall.Kill(-pgid, syscall.SIGKILL); err == nil {
			log.Printf("[ProcessGC] Successfully signaled SIGKILL to process group %d", pgid)
			return nil
		}
	}

	// Fallback to sending SIGKILL directly to PID
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("process with PID %d not found: %w", pid, err)
	}

	if err := proc.Kill(); err != nil && !os.IsNotExist(err) {
		log.Printf("[ProcessGC] Note: proc.Kill returned: %v (may already be terminated)", err)
	}

	// Grace period for OS cleanup
	time.Sleep(50 * time.Millisecond)
	return nil
}

// EnsureCleanSession checks if PID is still active and terminates if necessary.
func (gc *ProcessGC) EnsureCleanSession(pid int) {
	if pid <= 0 {
		return
	}
	// Signal 0 tests whether process exists
	if err := syscall.Kill(pid, 0); err == nil {
		log.Printf("[ProcessGC] PID %d is still alive after session end; forcing SIGKILL", pid)
		_ = gc.KillProcessTree(pid)
	}
}
