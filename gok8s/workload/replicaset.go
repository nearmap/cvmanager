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
	TypeReplicaSet = "ReplicaSet"
)

type ReplicaSet struct {
	replicaSet *appsv1.ReplicaSet

	client goappsv1.ReplicaSetInterface
}

func NewReplicaSet(cs kubernetes.Interface, namespace string, replicaSet *appsv1.ReplicaSet) *ReplicaSet {
	client := cs.AppsV1().ReplicaSets(namespace)
	return newReplicaSet(replicaSet, client)
}

func newReplicaSet(replicaSet *appsv1.ReplicaSet, client goappsv1.ReplicaSetInterface) *ReplicaSet {
	return &ReplicaSet{
		replicaSet: replicaSet,
		client:     client,
	}
}

func (rs *ReplicaSet) String() string {
	return fmt.Sprintf("%+v", rs.replicaSet)
}

// Name implements the Workload interface.
func (rs *ReplicaSet) Name() string {
	return rs.replicaSet.Name
}

// Namespace implements the Workload interface.
func (rs *ReplicaSet) Namespace() string {
	return rs.replicaSet.Namespace
}

// Type implements the Workload interface.
func (rs *ReplicaSet) Type() string {
	return TypeReplicaSet
}

// PodSpec implements the Workload interface.
func (rs *ReplicaSet) PodSpec() corev1.PodSpec {
	return rs.replicaSet.Spec.Template.Spec
}

// RollbackAfter implements the Workload interface.
func (rs *ReplicaSet) RollbackAfter() *time.Duration {
	return nil
}

// ProgressHealth implements the Workload interface.
func (rs *ReplicaSet) ProgressHealth(startTime time.Time) (*bool, error) {
	result := true
	return &result, nil
}

// PodTemplateSpec implements the TemplateRolloutTarget interface.
func (rs *ReplicaSet) PodTemplateSpec() corev1.PodTemplateSpec {
	return rs.replicaSet.Spec.Template
}

// PatchPodSpec implements the Workload interface.
func (rs *ReplicaSet) PatchPodSpec(cv *cv1.ContainerVersion, container corev1.Container, version string) error {
	_, err := rs.client.Patch(rs.replicaSet.ObjectMeta.Name, types.StrategicMergePatchType,
		[]byte(fmt.Sprintf(podTemplateSpecJSON, container.Name, cv.Spec.ImageRepo, version)))
	if err != nil {
		return errors.Wrapf(err, "failed to patch pod template spec container for ReplicaSet %s", rs.replicaSet.Name)
	}
	return nil
}

// AsResource implements the Workload interface.
func (rs *ReplicaSet) AsResource(cv *cv1.ContainerVersion) *Resource {
	for _, c := range rs.replicaSet.Spec.Template.Spec.Containers {
		if cv.Spec.Container.Name == c.Name {
			return &Resource{
				Namespace: cv.Namespace,
				Name:      rs.replicaSet.Name,
				Type:      TypeReplicaSet,
				Container: c.Name,
				Version:   version(c.Image),
				CV:        cv.Name,
				Tag:       cv.Spec.Tag,
			}
		}
	}

	return nil
}
