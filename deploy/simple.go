package deploy

import (
	"context"
	"log"
	"strings"
	"time"

	cv1 "github.com/nearmap/cvmanager/gok8s/apis/custom/v1"
	"github.com/nearmap/cvmanager/state"
	"github.com/pkg/errors"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
)

// SimpleDeployer implements a rollout strategy by patching the target's pod spec with a new version.
type SimpleDeployer struct {
	cv            *cv1.ContainerVersion
	version       string
	target        RolloutTarget
	checkRollback bool

	next state.State
}

// NewSimpleDeployer returns a new SimpleDeployer instance, which triggers rollouts
// by patching the target's pod spec with a new version and using the default
// Kubernetes deployment strategy for the workload.
func NewSimpleDeployer(cv *cv1.ContainerVersion, version string, target RolloutTarget, checkRollback bool,
	next state.State) *SimpleDeployer {

	log.Printf("Creating SimpleDeployer: cv=%s, version=%s, target=%s", cv.Name, version, target.Name())

	return &SimpleDeployer{
		cv:            cv,
		version:       version,
		target:        target,
		checkRollback: checkRollback,
		next:          next,
	}
}

// Do implements the state interface.
func (sd *SimpleDeployer) Do(ctx context.Context) (state.States, error) {
	log.Printf("Performing simple deployment: target=%s, version=%s", sd.target.Name(), sd.version)

	var container *v1.Container
	podSpec := sd.target.PodSpec()

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		for _, c := range podSpec.Containers {
			if c.Name == sd.cv.Spec.Container.Name {
				if updateErr := sd.target.PatchPodSpec(sd.cv, c, sd.version); updateErr != nil {
					log.Printf("Failed to update container version: version=%v, target=%v, error=%v",
						sd.version, sd.target.Name(), updateErr)
					return updateErr
				}
				container = &c
			}
		}
		return nil
	})
	if err == nil && container == nil {
		err = errors.Errorf("container with name %s not found in PodSpec for target %s",
			sd.cv.Spec.Container, sd.target.Name())
	}
	if err != nil {
		log.Printf("Failed to rollout: target=%s, version=%s, error=%v", sd.target.Name(), sd.version, err)
		return state.Error(err)
	}

	if sd.checkRollback {
		return state.Single(sd.checkRollbackState(container, sd.next))
	}

	return state.Single(sd.next)
}

func (sd *SimpleDeployer) checkRollbackState(container *v1.Container, next state.State) state.StateFunc {
	return func(ctx context.Context) (state.States, error) {
		if sd.target.RollbackAfter() == nil {
			return state.Single(next)
		}

		prevVersion := strings.SplitAfterN(container.Image, ":", 2)[1]

		if time.Now().UTC().Before(state.StartTime(ctx).Add(*sd.target.RollbackAfter())) {
			return state.After(time.Second*15, sd.checkRollbackState(container, next))
		}

		if sd.target.ProgressHealth() {
			return state.Single(next)
		}

		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if rbErr := sd.target.PatchPodSpec(sd.cv, *container, prevVersion); rbErr != nil {
				log.Printf(`Failed to rollback container version (will retry):
						from version=%s, to version=%s, target=%s, error=%v`,
					sd.version, prevVersion, sd.target.Name(), rbErr)
				return rbErr
			}
			return nil
		})
		if err != nil {
			return state.Error(err)
		}

		return state.Single(next)
	}
}
