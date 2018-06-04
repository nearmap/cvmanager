package fake

import (
	"fmt"
	"reflect"
	"time"

	"github.com/nearmap/cvmanager/deploy"
	cv1 "github.com/nearmap/cvmanager/gok8s/apis/custom/v1"
	corev1 "k8s.io/api/core/v1"
)

// RolloutTarget defines a fake RolloutTarget implementation for use in testing.
type RolloutTarget struct {
	FakeName           string
	FakeNamespace      string
	FakeType           string
	FakePodSpec        corev1.PodSpec
	FakeRollbackAfter  *time.Duration
	FakeProgressHealth *bool

	Invocations chan interface{}
}

// TemplateRolloutTarget defines a fake TemplateRolloutTarget implementation for
// use in testing.
type TemplateRolloutTarget struct {
	RolloutTarget

	FakePodTemplateSpec corev1.PodTemplateSpec
	FakeNumReplicas     int32
}

// NewRolloutTarget returns a RolloutTarget instance for use in testing.
func NewRolloutTarget() *RolloutTarget {
	return &RolloutTarget{
		Invocations: make(chan interface{}, 100),
	}
}

// NewTemplateRolloutTarget returns a TemplateRolloutTarget instance for use
// in testing.
func NewTemplateRolloutTarget() *TemplateRolloutTarget {
	return &TemplateRolloutTarget{
		RolloutTarget: RolloutTarget{
			Invocations: make(chan interface{}, 100),
		},
	}
}

// Name implements the RolloutTarget interface.
func (rt *RolloutTarget) Name() string {
	return rt.FakeName
}

// Namespace implements the RolloutTarget interface.
func (rt *RolloutTarget) Namespace() string {
	return rt.FakeNamespace
}

// Type implements the RolloutTarget interface.
func (rt *RolloutTarget) Type() string {
	return rt.FakeType
}

// PodSpec implements the RolloutTarget interface.
func (rt *RolloutTarget) PodSpec() corev1.PodSpec {
	return rt.FakePodSpec
}

// ReceivedPatchPodSpec represents the received parameters of an invocation of the
// PatchPodSpec method.
type ReceivedPatchPodSpec struct {
	CV        *cv1.ContainerVersion
	Container corev1.Container
	Version   string
}

// InvocationPatchPodSpec represents an invocation of the PatchPodSpec method.
type InvocationPatchPodSpec struct {
	Received *ReceivedPatchPodSpec

	Error error
}

// NewInvocationPatchPodSpec returns an invocation instance of the PatchPodSpec method.
func NewInvocationPatchPodSpec() *InvocationPatchPodSpec {
	return &InvocationPatchPodSpec{
		Received: &ReceivedPatchPodSpec{},
	}
}

// PatchPodSpec implements the RolloutTarget interface.
func (rt *RolloutTarget) PatchPodSpec(cv *cv1.ContainerVersion, container corev1.Container, version string) error {
	var pps InvocationPatchPodSpec
	rt.invocationFor(&pps)

	if pps.Received != nil {
		pps.Received.CV = cv
		pps.Received.Container = container
		pps.Received.Version = version
	}

	return pps.Error
}

// RollbackAfter implements the RolloutTarget interface.
func (rt *RolloutTarget) RollbackAfter() *time.Duration {
	return rt.FakeRollbackAfter
}

// ProgressHealth implements the ProgressHealth interface.
func (rt *RolloutTarget) ProgressHealth(startTime time.Time) (*bool, error) {
	return rt.FakeProgressHealth, nil
}

// PodTemplateSpec implements the TemplateRolloutTarget interface.
func (trt *TemplateRolloutTarget) PodTemplateSpec() corev1.PodTemplateSpec {
	return trt.FakePodTemplateSpec
}

// ReceivedSelect represents the received parameters of an invocation of the Select method.
type ReceivedSelect struct {
	Selector map[string]string
}

// InvocationSelect represents an invocation of the Select method.
type InvocationSelect struct {
	Received *ReceivedSelect

	ReturnTargets []deploy.TemplateRolloutTarget
	ReturnError   error
}

// NewInvocationSelect returns an instance of an InvocationSelect.
func NewInvocationSelect() *InvocationSelect {
	return &InvocationSelect{
		Received: &ReceivedSelect{},
	}
}

// Select implements the TemplateRolloutTarget interface.
func (trt *TemplateRolloutTarget) Select(selector map[string]string) ([]deploy.TemplateRolloutTarget, error) {
	var is InvocationSelect
	trt.invocationFor(&is)

	if is.Received != nil {
		is.Received.Selector = selector
	}

	return is.ReturnTargets, is.ReturnError
}

// ReceivedSelectOwnPods represents the received values for an invocation of the
// SelectOwnPods method.
type ReceivedSelectOwnPods struct {
	Pods []corev1.Pod
}

// InvocationSelectOwnPods represents an invocation of the SelectOwnPods method.
type InvocationSelectOwnPods struct {
	Received *ReceivedSelectOwnPods

	ReturnPods  []corev1.Pod
	ReturnError error
}

// NewInvocationSelectOwnPods returns a new instance of InvocationSelectOwnPods.
func NewInvocationSelectOwnPods() *InvocationSelectOwnPods {
	return &InvocationSelectOwnPods{
		Received: &ReceivedSelectOwnPods{},
	}
}

// SelectOwnPods implements the TemplateRolloutTarget interface.
func (trt *TemplateRolloutTarget) SelectOwnPods(pods []corev1.Pod) ([]corev1.Pod, error) {
	var sop InvocationSelectOwnPods
	trt.invocationFor(&sop)

	if sop.Received != nil {
		sop.Received.Pods = pods
	}

	return sop.ReturnPods, sop.ReturnError
}

// NumReplicas implements the TemplateRolloutTarget interface.
func (trt *TemplateRolloutTarget) NumReplicas() int32 {
	return trt.FakeNumReplicas
}

// ReceivedPatchNumReplicas represents the received values for a PatchNumReplicas invocation.
type ReceivedPatchNumReplicas struct {
	Num int32
}

// InvocationPatchNumReplicas represents an invocation of the PatchNumReplicas method.
type InvocationPatchNumReplicas struct {
	Received *ReceivedPatchNumReplicas

	Error error
}

// PatchNumReplicas implements the TemplateRolloutTarget interface.
func (trt *TemplateRolloutTarget) PatchNumReplicas(num int32) error {
	var pnr InvocationPatchNumReplicas
	trt.invocationFor(&pnr)

	if pnr.Received != nil {
		pnr.Received.Num = num
	}

	return pnr.Error
}

func (rt *RolloutTarget) invocationFor(invType interface{}) {
	var inv interface{}
	select {
	case inv = <-rt.Invocations:
		break
	default:
		panic("No invocations available")
	}

	typVal := reflect.ValueOf(invType)
	invVal := reflect.ValueOf(inv)

	if typVal.Type() != invVal.Type() {
		panic(fmt.Sprintf("received invocation of type %T but expected %T", inv, invType))
	}
	if typVal.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("expected invocation type to be a pointer"))
	}

	typVal.Elem().Set(invVal.Elem())
}
