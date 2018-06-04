package registry

import (
	"context"
	"strings"
)

// ProviderByRepo generates Type based on image ARN
func ProviderByRepo(repoARN string) string {
	if strings.Contains(repoARN, "amazonaws.com") {
		return "ecr"
	}
	return "dockerhub"
}

// Syncer offers capability to periodically sync with docker registry
//type Syncer interface {
//	Sync() error
//}

// Tagger provides capability of adding/removing environment tags on ECR
// This interface is purely designed for CI/CD purposes such that the version
// tag ex git SHA is unique on images (images can be uniquely identified by such version tags).
// Environment tags or any other tags are then added or removed from the ECR images.
type Tagger interface {
	// Add adds list of tags to the image identified with version
	Add(version string, tags ...string) error
	// Remove removes the list of tags from ECR repository such that no image contains these
	// tags
	Remove(tags ...string) error
	// Get gets the list of tags to the image identified with version
	Get(version string) ([]string, error)
}

type Registry interface {
	Version(ctx context.Context, tag string) (string, error)
}
