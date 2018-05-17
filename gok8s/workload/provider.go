package k8s

import (
	"strings"

	"github.com/nearmap/cvmanager/config"
	"github.com/nearmap/cvmanager/deploy"
	cv1 "github.com/nearmap/cvmanager/gok8s/apis/custom/v1"
	clientset "github.com/nearmap/cvmanager/gok8s/client/clientset/versioned"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// Workload defines an interface for something deployable, such as a Deployment, DaemonSet, Pod, etc.
type Workload interface {
	deploy.RolloutTarget

	// AsResource returns a Resource struct defining the current state of the workload.
	AsResource(cv *cv1.ContainerVersion) *Resource
}

// Resource maintains a high level status of deployments managed by
// CV resources including version of current deploy and number of available pods
// from this deployment/replicaset
type Resource struct {
	Namespace     string
	Name          string
	Type          string
	Container     string
	Version       string
	AvailablePods int32

	CV  string
	Tag string
}

// Provider manages workloads.
type Provider struct {
	cs        kubernetes.Interface
	cvcs      clientset.Interface
	namespace string

	//hp      history.Provider
	options *config.Options
}

// NewProvider abstracts operations performed against Kubernetes resources such as syncing deployments
// config maps etc
func NewProvider(cs kubernetes.Interface, cvcs clientset.Interface, ns string, options ...func(*config.Options)) *Provider {
	opts := config.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	return &Provider{
		cs:        cs,
		cvcs:      cvcs,
		namespace: ns,
		options:   opts,

		//hp: history.NewProvider(cs, opts.Stats),
	}
}

// Namespace returns the namespace that this K8sProvider is operating within.
func (k *Provider) Namespace() string {
	return k.namespace
}

// Client returns a kubernetes client interface for working directly with the kubernetes API.
// The client will only work within the namespace of the provider.
func (k *Provider) Client() kubernetes.Interface {
	return k.cs
}

/*
func (k *Provider) deploy(cv *cv1.ContainerVersion, version string, target deploy.RolloutTarget) error {
	if ok := k.checkRolloutStatus(cv, version, target); !ok {
		return nil
	}

	var deployer deploy.Deployer

	var kind string
	if cv.Spec.Strategy != nil {
		kind = cv.Spec.Strategy.Kind
	}

	switch kind {
	case deploy.KindServieBlueGreen:
		deployer = deploy.NewBlueGreenDeployer(k.cs, k.namespace, config.WithOptions(k.options))
	default:
		deployer = deploy.NewSimpleDeployer(k.namespace, config.WithOptions(k.options))
	}

	if err := deployer.Deploy(cv, version, target); err != nil {
		if deploy.IsPermanent(err) {
			log.Printf("Permanent failure from deploy operation: %v", err)
			if _, e := k.updateFailedRollouts(cv, version); e != nil {
				log.Printf("Failed to update state for container version %s: %v", cv.Name, e)
			}
		}
		return errors.WithStack(err)
	}
	return nil
}

// checkRolloutStatus determines whether a rollout should occur for the target.
// TODO: put the version check and this logic in a single place with a single
// shouldPerformRollout() style method.
func (k *Provider) checkRolloutStatus(cv *cv1.ContainerVersion, version string, target deploy.RolloutTarget) bool {
	maxAttempts := cv.Spec.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	if numFailed := cv.Status.FailedRollouts[version]; numFailed >= maxAttempts {
		log.Printf("Not attempting rollout: cv spec %s for version %s has failed %d times.", cv.Name, version, numFailed)
		return false
	}

	return true
}

func (k *Provider) updateFailedRollouts(cv *cv1.ContainerVersion, version string) (*cv1.ContainerVersion, error) {
	client := k.cvcs.CustomV1().ContainerVersions(k.namespace)

	// ensure state is up to date
	spec, err := client.Get(cv.Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get ContainerVersion instance with name %s", cv.Name)
	}
	if spec.Status.FailedRollouts == nil {
		spec.Status.FailedRollouts = map[string]int{}
	}
	spec.Status.FailedRollouts[version] = spec.Status.FailedRollouts[version] + 1

	result, err := client.Update(spec)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to update ContainerVersion spec %s", cv.Name)
	}

	log.Printf("Updated failed rollouts for cv spec=%s, version=%s, numFailures=%d",
		cv.Name, version, cv.Status.FailedRollouts[version])

	return result, nil
}
*/

// AllResources returns all resources managed by container versions in the current namespace.
func (k *Provider) AllResources() ([]*Resource, error) {
	cvs, err := k.cvcs.CustomV1().ContainerVersions("").List(metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to generate template of CV list")
	}

	var cvsList []*Resource
	for _, cv := range cvs.Items {
		cvs, err := k.CVResources(&cv)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to generate template of CV list")
		}
		cvsList = append(cvsList, cvs...)
	}

	return cvsList, nil
}

// CVResources returns the resources managed by the given cv instance.
func (k *Provider) CVResources(cv *cv1.ContainerVersion) ([]*Resource, error) {
	var resources []*Resource

	specs, err := k.Workloads(cv)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for _, spec := range specs {
		resources = append(resources, spec.AsResource(cv))
	}

	return resources, nil
}

// Workloads returns the workload instances that match the given container version resource.
func (k *Provider) Workloads(cv *cv1.ContainerVersion) ([]Workload, error) {
	var result []Workload

	set := labels.Set(cv.Spec.Selector)
	listOpts := metav1.ListOptions{LabelSelector: set.AsSelector().String()}

	deployments, err := k.cs.AppsV1().Deployments(k.namespace).List(listOpts)
	if err != nil {
		return nil, k.handleError(err, "deployments")
	}
	for _, item := range deployments.Items {
		wl := item
		result = append(result, NewDeployment(k.cs, k.namespace, &wl))
	}

	cronJobs, err := k.cs.BatchV1beta1().CronJobs(k.namespace).List(listOpts)
	if err != nil {
		return nil, k.handleError(err, "cronJobs")
	} else {
		for _, item := range cronJobs.Items {
			wl := item
			result = append(result, NewCronJob(k.cs, k.namespace, &wl))
		}
	}

	daemonSets, err := k.cs.AppsV1().DaemonSets(k.namespace).List(listOpts)
	if err != nil {
		return nil, k.handleError(err, "daemonSets")
	}
	for _, item := range daemonSets.Items {
		wl := item
		result = append(result, NewDaemonSet(k.cs, k.namespace, &wl))
	}

	jobs, err := k.cs.BatchV1().Jobs(k.namespace).List(listOpts)
	if err != nil {
		return nil, k.handleError(err, "jobs")
	}
	for _, item := range jobs.Items {
		wl := item
		result = append(result, NewJob(k.cs, k.namespace, &wl))
	}

	pods, err := k.cs.CoreV1().Pods(k.namespace).List(listOpts)
	if err != nil {
		return nil, k.handleError(err, "pods")
	}
	for _, item := range pods.Items {
		wl := item
		result = append(result, NewPod(k.cs, k.namespace, &wl))
	}

	replicaSets, err := k.cs.AppsV1().ReplicaSets(k.namespace).List(listOpts)
	if err != nil {
		return nil, k.handleError(err, "replicaSets")
	}
	for _, item := range replicaSets.Items {
		wl := item
		result = append(result, NewReplicaSet(k.cs, k.namespace, &wl))
	}

	statefulSets, err := k.cs.AppsV1().StatefulSets(k.namespace).List(listOpts)
	if err != nil {
		return nil, k.handleError(err, "statefulSets")
	}
	for _, item := range statefulSets.Items {
		wl := item
		result = append(result, NewStatefulSet(k.cs, k.namespace, &wl))
	}

	return result, nil
}

func (k *Provider) handleError(err error, typ string) error {
	//k.options.Recorder.Event(events.Warning, "CRSyncFailed", "Failed to get workload")
	return errors.Wrapf(err, "failed to get %s", typ)
}

/*
func (k *Provider) validate(v string, cvvs []*cv1.VerifySpec) error {
	for _, v := range cvvs {
		verifier, err := verify.NewVerifier(k.cs, k.options.Recorder, k.options.Stats, k.namespace, v)
		if err != nil {
			return errors.WithStack(err)
		}
		if err = verifier.Verify(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
*/

func version(img string) string {
	return strings.SplitAfterN(img, ":", 2)[1]
}
