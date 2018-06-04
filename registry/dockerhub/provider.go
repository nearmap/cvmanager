package dockerhub

import (
	"context"
	"fmt"

	"github.com/heroku/docker-registry-client/registry"
	"github.com/nearmap/cvmanager/stats"
	"github.com/pkg/errors"
)

const dockerhubURL = "https://registry-1.docker.io/"

// Options contains additional (optional) configuration for the controller
type Options struct {
	Stats stats.Stats

	HubURL, User, Password string
}

// WithStats applies the stats type to the controller
func WithStats(instance stats.Stats) func(*Options) {
	return func(opts *Options) {
		opts.Stats = instance
	}
}

// WithCreds applies the stats type to the controller
func WithCreds(user, password string) func(*Options) {
	return func(opts *Options) {
		opts.User = user
		opts.Password = password
	}
}

// syncer is responsible to syncing with the docker registry (dr) repository and
// ensuring that the deployment it is monitoring is up to date. If it finds
// the deployment outdated from what Tag is indicating the deployment version should be.
// it performs an update in deployment which then based on update strategy of deployment
// is further rolled out.
// In cases, where it cant resolves
// TODO: Dockerhub syncer does not support private repositories at the moment
// - Need to work on storing credentials in secret or similar more secure approach
// for user/password. For now uses anonymous access
// - Need to define auth mechanism
type dhV2Provider struct {
	repository string
	client     *registry.Registry
	opts       *Options
}

// NewSyncer provides new reference of syncer
// to manage Dockerhub repository and sync deployments periodically
func NewDH(repository, versionExp string, options ...func(*Options)) (*dhV2Provider, error) {
	opts := &Options{
		Stats:    stats.NewFake(),
		User:     "",
		Password: "",
		HubURL:   dockerhubURL,
	}

	for _, opt := range options {
		opt(opts)
	}

	client, err := registry.New(opts.HubURL, opts.User, opts.Password)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to connect to dockerhub")
	}
	dhV2Provider := &dhV2Provider{
		client:     client,
		repository: repository,
		opts:       opts,
	}

	return dhV2Provider, nil
}

func (s *dhV2Provider) Version(ctx context.Context, tag string) (string, error) {
	newVersion, err := s.getDigest(tag)
	if err != nil {
		s.opts.Stats.IncCount(fmt.Sprintf("registry.%s.sync.failure", s.repository), "badsha")
		return "", errors.Errorf("No version found for tag %s", tag)
	}
	return newVersion, nil
}

// Add adds list of tags to the image identified with version
func (s *dhV2Provider) Add(version string, tags ...string) error {
	return s.addTagsOnImg(version, tags...)
}

// Remove removes the list of tags from dockerhub repository such that no image contains these
// tags
func (s *dhV2Provider) Remove(tags ...string) error {
	return errors.New(`Dockerhub does not support multiple tags an image and
		thus removing a subset from it is not supported`)
}

// Get gets the list of tags to the image identified with version
func (s *dhV2Provider) Get(version string) ([]string, error) {
	digest, err := s.getDigest(version)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to connect to dockerhub")
	}
	return []string{version, digest}, nil
}

// addTagsOnImg fetches the manifest of container image of specified version tag
// and tag additional tags to the same manifest
// TODO:  allow using credentials to support private dockerhub repos too
// for now uses anonymous access
func (s *dhV2Provider) addTagsOnImg(version string, tags ...string) error {
	manifest, err := s.client.Manifest(s.repository, version)
	if err != nil {
		return errors.Wrapf(err, "Failed to find manifest for image version %s on repository %s", version, s.repository)
	}

	for _, tag := range tags {
		err := s.client.PutManifest(s.repository, tag, manifest)
		if err != nil {
			return errors.Wrapf(err, "Failed to add tags %s on image version %s on repository %s", tag, version, s.repository)
		}
	}
	return nil
}

// getDigest fetches the digest of dockerhub image of requested repository and tag
func (s *dhV2Provider) getDigest(tag string) (string, error) {
	digest, err := s.client.ManifestDigest(s.repository, tag)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get tag %s on repository %s", tag, s.repository)
	}
	return digest.String(), nil
}
