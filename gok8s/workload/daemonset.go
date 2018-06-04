package k8s

import (
	"fmt"
	"time"

	cv1 "github.com/nearmap/cvmanager/gok8s/apis/custom/v1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	goappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
)

const (
	TypeDaemonSet = "DaemonSet"
)

type DaemonSet struct {
	daemonSet *appsv1.DaemonSet

	client goappsv1.DaemonSetInterface
}

func NewDaemonSet(cs kubernetes.Interface, namespace string, daemonSet *appsv1.DaemonSet) *DaemonSet {
	client := cs.AppsV1().DaemonSets(namespace)
	return newDaemonSet(daemonSet, client)
}

func newDaemonSet(daemonSet *appsv1.DaemonSet, client goappsv1.DaemonSetInterface) *DaemonSet {
	return &DaemonSet{
		daemonSet: daemonSet,
		client:    client,
	}
}

func (ds *DaemonSet) String() string {
	return fmt.Sprintf("%+v", ds.daemonSet)
}

// Name implements the Workload interface.
func (ds *DaemonSet) Name() string {
	return ds.daemonSet.Name
}

// Namespace implements the Workload interface.
func (ds *DaemonSet) Namespace() string {
	return ds.daemonSet.Namespace
}

// Type implements the Workload interface.
func (ds *DaemonSet) Type() string {
	return TypeDaemonSet
}

// PodSpec implements the Workload interface.
func (ds *DaemonSet) PodSpec() corev1.PodSpec {
	return ds.daemonSet.Spec.Template.Spec
}

// RollbackAfter implements the Workload interface.
func (ds *DaemonSet) RollbackAfter() *time.Duration {
	return nil
}

// ProgressHealth implements the Workload interface.
func (ds *DaemonSet) ProgressHealth(startTime time.Time) (*bool, error) {
	result := true
	return &result, nil
}

// PodTemplateSpec implements the TemplateRolloutTarget interface.
func (ds *DaemonSet) PodTemplateSpec() corev1.PodTemplateSpec {
	return ds.daemonSet.Spec.Template
}

// PatchPodSpec implements the Workload interface.
func (ds *DaemonSet) PatchPodSpec(cv *cv1.ContainerVersion, container corev1.Container, version string) error {
	_, err := ds.client.Patch(ds.daemonSet.ObjectMeta.Name, types.StrategicMergePatchType,
		[]byte(fmt.Sprintf(podTemplateSpecJSON, container.Name, cv.Spec.ImageRepo, version)))
	if err != nil {
		return errors.Wrapf(err, "failed to patch pod template spec container for DaemonSet %s", ds.daemonSet.Name)
	}
	return nil
}

// AsResource implements the Workload interface.
func (ds *DaemonSet) AsResource(cv *cv1.ContainerVersion) *Resource {
	for _, c := range ds.daemonSet.Spec.Template.Spec.Containers {
		if cv.Spec.Container.Name == c.Name {
			return &Resource{
				Namespace: cv.Namespace,
				Name:      ds.daemonSet.Name,
				Type:      TypeDaemonSet,
				Container: c.Name,
				Version:   version(c.Image),
				CV:        cv.Name,
				Tag:       cv.Spec.Tag,
			}
		}
	}

	return nil
}
