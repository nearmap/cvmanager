package deploy

import (
	"context"
	"strings"
	"time"

	"github.com/golang/glog"
	cv1 "github.com/nearmap/cvmanager/gok8s/apis/custom/v1"
	"github.com/nearmap/cvmanager/state"
	"github.com/pkg/errors"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
)

// SimpleDeployer implements a rollout strategy by patching the target's pod spec with a new version.
type SimpleDeployer struct {
	cv      *cv1.ContainerVersion
	version string
	target  RolloutTarget

	checkRollback bool

	next state.State
}

// NewSimpleDeployer returns a new SimpleDeployer instance, which triggers rollouts
// by patching the target's pod spec with a new version and using the default
// Kubernetes deployment strategy for the workload.
func NewSimpleDeployer(cv *cv1.ContainerVersion, version string, target RolloutTarget, checkRollback bool,
	next state.State) *SimpleDeployer {

	glog.V(2).Infof("Creating SimpleDeployer: cv=%s, version=%s, target=%s", cv.Name, version, target.Name())

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
	glog.V(2).Infof("Performing simple deployment: target=%s, version=%s", sd.target.Name(), sd.version)

	var container *v1.Container
	podSpec := sd.target.PodSpec()

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		for _, c := range podSpec.Containers {
			if c.Name == sd.cv.Spec.Container.Name {
				if updateErr := sd.target.PatchPodSpec(sd.cv, c, sd.version); updateErr != nil {
					glog.V(2).Infof("Failed to update container version: version=%v, target=%v, error=%v",
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
		glog.V(2).Infof("Failed to rollout: target=%s, version=%s, error=%v", sd.target.Name(), sd.version, err)
		return state.Error(err)
	}

	if sd.checkRollback {
		return state.Single(sd.checkRollbackState(container, sd.next))
	}

	glog.V(2).Infof("Not checking rollback state")
	return state.Single(sd.next)
}

func (sd *SimpleDeployer) checkRollbackState(container *v1.Container, next state.State) state.StateFunc {
	return func(ctx context.Context) (state.States, error) {
		if sd.target.RollbackAfter() == nil {
			glog.V(2).Infof("Target %s does not define a progress deadline.", sd.target.Name())
			return state.Single(next)
		}

		healthy, err := sd.target.ProgressHealth(sd.cv.Status.CurrStatusTime.Time)
		if err != nil {
			return state.Error(errors.Wrapf(err, "failed to check progress health for %s", sd.target.Name()))
		}
		if healthy == nil {
			glog.V(4).Infof("Waiting for healthy state of target %s", sd.target.Name())
			return state.After(time.Second*15, sd.checkRollbackState(container, next))
		}

		if *healthy == true {
			glog.V(2).Info("Target is healthy")
			return state.Single(next)
		}

		// rollback
		prevVersion := strings.SplitAfterN(container.Image, ":", 2)[1]
		glog.V(1).Infof("Rolling back target %s", sd.target.Name())
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if rbErr := sd.target.PatchPodSpec(sd.cv, *container, prevVersion); rbErr != nil {
				glog.V(2).Infof("Failed to rollback container version (will retry):	from version=%s, to version=%s, target=%s, error=%v",
					sd.version, prevVersion, sd.target.Name(), rbErr)
				return rbErr
			}
			return nil
		})
		if err != nil {
			return state.Error(err)
		}

		return state.Error(state.NewFailed("deployment failed healthy state check and was rolled back"))
	}
}
