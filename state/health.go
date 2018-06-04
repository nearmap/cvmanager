package state

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
)

const (
	healthFilename = "/tmp/cvm-state"
)

// UpdateHealthStatus updates the health status of the syncer to indicate that it is
// in a healthy state.
func UpdateHealthStatus() error {
	if err := ioutil.WriteFile(healthFilename, []byte(""), 0755); err != nil {
		return errors.Wrapf(err, "failed to write health file at %s", healthFilename)
	}

	return nil
}

// CleanupHealthStatus removes the existing health state.
func CleanupHealthStatus() error {
	if err := os.Remove(healthFilename); err != nil {
		return errors.Wrapf(err, "failed to clean up health status at %s", healthFilename)
	}
	return nil
}

// CheckHealth checks that the last health update was within the given time duration.
// Returns nil if the last update run was run within the duration, and an error otherwise.
func CheckHealth(d time.Duration) error {
	f, err := os.Stat(healthFilename)
	if err != nil {
		glog.Errorf("Failed to stat health file at %s: %v", healthFilename, err)
		return errors.Wrapf(err, "failed to stat the health status at %s", healthFilename)
	}

	if !time.Now().Add(d * -1).Before(f.ModTime()) {
		glog.Errorf("Failed health check: last health update was at %v", f.ModTime())
		return errors.Errorf("Last health update was %s", f.ModTime().String())
	}

	return nil
}
