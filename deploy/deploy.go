package deploy

import (
	"time"

	"github.com/golang/glog"
	cv1 "github.com/nearmap/cvmanager/gok8s/apis/custom/v1"
	"github.com/nearmap/cvmanager/state"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	RolloutStatusFailed      = "Failed"
	RolloutStatusSuccess     = "Success"
	RolloutStatusProgressing = "Progressing"
)

// RolloutTarget defines an interface for something deployable, such as a Deployment, DaemonSet, Pod, etc.
type RolloutTarget interface {
	// Name is the name of the workload (without the namespace).
	Name() string

	// Namespace returns the namespace the workload belongs to.
	Namespace() string

	// Type returns the type of the spec.
	Type() string

	// PodSpec returns the PodSpec for the workload.
	PodSpec() corev1.PodSpec

	// PatchPodSpec receives a pod spec and container which is to be patched
	// according to an appropriate strategy for the type.
	PatchPodSpec(cv *cv1.ContainerVersion, container corev1.Container, version string) error

	// RollbackAfter indicates duration after which a failed rollout
	// should attempt rollback
	RollbackAfter() *time.Duration

	// ProgressHealth indicates weather the current status of progress healthy or not.
	// The start time of the deployment operation is provided.
	ProgressHealth(startTime time.Time) (*bool, error)
}

// TemplateRolloutTarget defines methods for deployable resources that manage a collection
// of pods via a pod template. More deployment options are available for such
// resources.
type TemplateRolloutTarget interface {
	RolloutTarget

	// PodTemplateSpec returns the PodTemplateSpec for this workload.
	PodTemplateSpec() corev1.PodTemplateSpec

	// Select all Workloads of this type with the given selector. May return
	// the current spec if it matches the selector.
	Select(selector map[string]string) ([]TemplateRolloutTarget, error)

	// SelectOwnPods returns a list of pods that are managed by this workload.
	SelectOwnPods(pods []corev1.Pod) ([]corev1.Pod, error)

	// NumReplicas returns the current number of running replicas for this workload.
	NumReplicas() int32

	// PatchNumReplicas modifies the number of replicas for this workload.
	PatchNumReplicas(num int32) error
}

// Deployer is an interface for rollout strategies.
type Deployer interface {
	// Deploy initiates a rollout for a target spec based on the underlying strategy implementation.
	Deploy(cv *cv1.ContainerVersion, version string, spec RolloutTarget) error
}

// NewDeployState returns a state that performs a deployment operation according to the
// ContainerVersion spec.
func NewDeployState(cs kubernetes.Interface, namespace string, cv *cv1.ContainerVersion, version string,
	target RolloutTarget, withRollback bool, next state.State) state.State {

	glog.V(2).Infof("Creating deployment for cv=%+v, version=%s, rolloutTarget=%s", cv, version, target.Name())

	var kind string
	if cv.Spec.Strategy != nil {
		kind = cv.Spec.Strategy.Kind
	}

	switch kind {
	case KindServieBlueGreen:
		return NewBlueGreenDeployer(cs, namespace, cv, version, target, next)
	default:
		return NewSimpleDeployer(cv, version, target, withRollback, next)
	}
}
