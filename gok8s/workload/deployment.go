package k8s

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/nearmap/cvmanager/deploy"
	cv1 "github.com/nearmap/cvmanager/gok8s/apis/custom/v1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	goappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
)

const (
	TypeDeployment = "Deployment"
)

type Deployment struct {
	deployment *appsv1.Deployment

	client           goappsv1.DeploymentInterface
	replicasetClient goappsv1.ReplicaSetInterface
}

func NewDeployment(cs kubernetes.Interface, namespace string, deployment *appsv1.Deployment) *Deployment {
	client := cs.AppsV1().Deployments(namespace)
	replicasetClient := cs.AppsV1().ReplicaSets(namespace)

	return newDeployment(deployment, client, replicasetClient)
}

func newDeployment(deployment *appsv1.Deployment, client goappsv1.DeploymentInterface,
	replicasetClient goappsv1.ReplicaSetInterface) *Deployment {

	return &Deployment{
		deployment:       deployment,
		client:           client,
		replicasetClient: replicasetClient,
	}
}

// curr returns the current state of the deployment.
func (d *Deployment) curr() (*appsv1.Deployment, error) {
	dep, err := d.client.Get(d.deployment.Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get deployment state")
	}
	return dep, nil
}

func (d *Deployment) String() string {
	return fmt.Sprintf("%+v", d.deployment)
}

// Name implements the Workload interface.
func (d *Deployment) Name() string {
	return d.deployment.Name
}

// Namespace implements the Workload interface.
func (d *Deployment) Namespace() string {
	return d.deployment.Namespace
}

// Type implements the Workload interface.
func (d *Deployment) Type() string {
	return TypeDeployment
}

// PodSpec implements the Workload interface.
func (d *Deployment) PodSpec() corev1.PodSpec {
	return d.deployment.Spec.Template.Spec
}

// RollbackAfter implements the Workload interface.
func (d *Deployment) RollbackAfter() *time.Duration {
	if d.deployment.Spec.ProgressDeadlineSeconds == nil {
		return nil
	}
	dur := time.Duration(*d.deployment.Spec.ProgressDeadlineSeconds) * time.Second
	return &dur
}

// ProgressHealth implements the Workload interface.
func (d *Deployment) ProgressHealth(startTime time.Time) (*bool, error) {
	glog.V(4).Infof("checking progress health at: %v", startTime)

	dep, err := d.curr()
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain progress health")
	}

	var ok *bool
	for _, c := range dep.Status.Conditions {
		if glog.V(4) {
			glog.V(4).Infof("deployment condition: %+v", c)
		}

		if c.LastUpdateTime.Time.Before(startTime) {
			continue
		}
		if c.Type != appsv1.DeploymentProgressing {
			continue
		}

		if c.Status == corev1.ConditionFalse {
			if c.Reason == "ProgressDeadlineExceeded" {
				result := false
				return &result, nil
			}
		} else {
			if c.Reason == "NewReplicaSetAvailable" {
				result := true
				ok = &result
			}
		}
	}

	return ok, nil
}

// PodTemplateSpec implements the TemplateRolloutTarget interface.
func (d *Deployment) PodTemplateSpec() corev1.PodTemplateSpec {
	return d.deployment.Spec.Template
}

// PatchPodSpec implements the Workload interface.
func (d *Deployment) PatchPodSpec(cv *cv1.ContainerVersion, container corev1.Container, version string) error {
	// TODO: should we update the deployment with the returned patch version?
	_, err := d.client.Patch(d.deployment.ObjectMeta.Name, types.StrategicMergePatchType,
		[]byte(fmt.Sprintf(podTemplateSpecJSON, container.Name, cv.Spec.ImageRepo, version)))
	if err != nil {
		return errors.Wrapf(err, "failed to patch pod template spec container for deployment %s", d.deployment.Name)
	}
	return nil
}

// Select implements the TemplateRolloutTarget interface.
func (d *Deployment) Select(selector map[string]string) ([]deploy.TemplateRolloutTarget, error) {
	set := labels.Set(selector)
	listOpts := metav1.ListOptions{LabelSelector: set.AsSelector().String()}

	var result []deploy.TemplateRolloutTarget

	wls, err := d.client.List(listOpts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, wl := range wls.Items {
		deployment := wl
		result = append(result, newDeployment(&deployment, d.client, d.replicasetClient))
	}

	return result, nil
}

// SelectOwnPods implements the TemplateRolloutTarget interface.
func (d *Deployment) SelectOwnPods(pods []corev1.Pod) ([]corev1.Pod, error) {
	var result []corev1.Pod
	for _, pod := range pods {
		for _, podOwner := range pod.OwnerReferences {
			switch podOwner.Kind {
			case TypeReplicaSet:
				rs, err := d.replicaSetForName(podOwner.Name)
				if err != nil {
					return nil, errors.Wrap(err, "failed to get pod's owner replicaset")
				}
				for _, rsOwner := range rs.OwnerReferences {
					switch rsOwner.Kind {
					case TypeDeployment:
						if rsOwner.Name == d.deployment.Name {
							result = append(result, pod)
						}
					default:
						glog.V(4).Infof("Ignoring unknown replicaset owner kind: %v", rsOwner.Kind)
					}
				}
			default:
				glog.V(4).Infof("Ignoring unknown pod owner kind: %v", podOwner.Kind)
			}
		}
	}

	glog.V(4).Info("SelectOwnPods returning %d pods", len(result))
	return result, nil
}

func (d *Deployment) replicaSetForName(name string) (*appsv1.ReplicaSet, error) {
	rs, err := d.replicasetClient.Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get replicaset with name %s", name)
	}

	return rs, nil
}

// NumReplicas implements the TemplateRolloutTarget interface.
func (d *Deployment) NumReplicas() int32 {
	return d.deployment.Status.Replicas
}

const deploymentReplicaSetPatchJSON = `
	{
		"spec": {
			"replicas": %d
		}
	}`

// PatchNumReplicas implements the TemplateRolloutTarget interface.
func (d *Deployment) PatchNumReplicas(num int32) error {
	_, err := d.client.Patch(d.deployment.ObjectMeta.Name, types.StrategicMergePatchType,
		[]byte(fmt.Sprintf(deploymentReplicaSetPatchJSON, num)))
	if err != nil {
		return errors.Wrapf(err, "failed to patch pod template spec replicas for deployment %s", d.deployment.Name)
	}
	return nil
}

// AsResource implements the Workload interface.
func (d *Deployment) AsResource(cv *cv1.ContainerVersion) *Resource {
	for _, c := range d.deployment.Spec.Template.Spec.Containers {
		if cv.Spec.Container.Name == c.Name {
			return &Resource{
				Namespace:     cv.Namespace,
				Name:          d.deployment.Name,
				Type:          TypeDeployment,
				Container:     c.Name,
				Version:       version(c.Image),
				AvailablePods: d.deployment.Status.AvailableReplicas,
				CV:            cv.Name,
				Tag:           cv.Spec.Tag,
			}
		}
	}

	return nil
}
