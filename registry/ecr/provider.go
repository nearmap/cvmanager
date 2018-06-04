package ecr

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/golang/glog"
	"github.com/nearmap/cvmanager/stats"
	"github.com/pkg/errors"
)

var ecrRule, _ = regexp.Compile("([0-9]*).dkr.ecr.([a-z0-9-]*).amazonaws.com/([a-zA-Z0-9/\\_-]*)")

// nameAccountRegionFromARN returns the name of the repo, the AWS Account ID and region
// that the repo belongs to from a given repo ARN.
func nameAccountRegionFromARN(arn string) (repoName, accountID, region string, err error) {
	rs := ecrRule.FindStringSubmatch(arn)
	if len(rs) != 4 {
		return "", "", "", errors.Errorf("ecr repo %s is not valid", arn)
	}
	return rs[3], rs[1], rs[2], nil
}

// ecrProvider is responsible to syncing with the ecr repository and
// ensuring that the deployment it is monitoring is up to date. If it finds
// the deployment outdated from what Tag is indicating the deployment version should be.
// it performs an update in deployment which then based on update strategy of deployment
// is further rolled out.
// In cases, where it cant resolves
type ecrProvider struct {
	sess      *session.Session
	ecr       *ecr.ECR
	repoName  string
	accountID string

	vRegex *regexp.Regexp

	stats stats.Stats
}

// NewSyncer provides new reference of dhSyncer
// to manage AWS ECR repository and sync deployments periodically
func NewECR(repository, versionExp string, stats stats.Stats) (*ecrProvider, error) {

	vRegex, err := regexp.Compile(versionExp)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	repoName, accountID, region, err := nameAccountRegionFromARN(repository) //cv.Spec.ImageRepo
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain AWS session")
	}

	ecrProvider := &ecrProvider{
		repoName:  repoName,
		accountID: accountID,
		sess:      sess,
		ecr:       ecr.New(sess, aws.NewConfig().WithRegion(region)),

		vRegex: vRegex,
		stats:  stats,
	}

	return ecrProvider, nil
}

func (s *ecrProvider) Version(ctx context.Context, tag string) (string, error) {
	// TODO: parameterize timeout
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	if glog.V(4) {
		glog.V(4).Infof("Making ECR DescribeImages request for repository=%s, registry=%s, tag=%s",
			s.repoName, s.accountID, tag)
	}

	req := &ecr.DescribeImagesInput{
		ImageIds: []*ecr.ImageIdentifier{
			{
				ImageTag: aws.String(tag),
			},
		},
		RegistryId:     aws.String(s.accountID),
		RepositoryName: aws.String(s.repoName),
	}
	result, err := s.ecr.DescribeImagesWithContext(ctx, req)
	if err != nil {
		glog.Errorf("Failed to get ECR: %v", err)
		return "", errors.Wrap(err, "failed to get ecr")
	}
	if len(result.ImageDetails) != 1 {
		s.stats.Event(fmt.Sprintf("registry.%s.sync.failure", s.repoName),
			fmt.Sprintf("Failed to sync with ECR for tag %s", tag), "", "error",
			time.Now().UTC(), tag)
		return "", errors.Errorf("Bad state: More than one image was tagged with %s", tag)
	}

	img := result.ImageDetails[0]

	currentVersion := s.currentVersion(img)
	if currentVersion == "" {
		s.stats.IncCount(fmt.Sprintf("registry.%s.sync.failure", s.repoName), "badsha")
		return "", errors.Errorf("No version found for tag %s", tag)
	}

	glog.V(2).Infof("Got currentVersion=%s from ECR", currentVersion)

	return currentVersion, nil
}

// Add adds list of tags to the image identified with version
func (s *ecrProvider) Add(version string, tags ...string) error {
	for _, tag := range tags {
		fmt.Printf("Tags are %s \n", tags)
		getReq := &ecr.BatchGetImageInput{
			ImageIds: []*ecr.ImageIdentifier{
				{
					ImageTag: aws.String(version),
				},
			},
			RegistryId:     aws.String(s.accountID),
			RepositoryName: aws.String(s.repoName),
		}

		getRes, err := s.ecr.BatchGetImage(getReq)
		if err != nil {
			s.stats.IncCount(fmt.Sprintf("registry.batchget.%s.failure", s.repoName))
			return errors.Wrap(err, fmt.Sprintf("failed to get images of tag %s", tag))
		}

		for _, img := range getRes.Images {
			putReq := &ecr.PutImageInput{
				ImageManifest:  img.ImageManifest,
				ImageTag:       aws.String(tag),
				RegistryId:     aws.String(s.accountID),
				RepositoryName: aws.String(s.repoName),
			}

			_, err = s.ecr.PutImage(putReq)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case ecr.ErrCodeImageAlreadyExistsException:
						continue
					}
				}
				s.stats.IncCount(fmt.Sprintf("registry.putimage.%s.failure", s.repoName))
				return errors.Wrap(err, fmt.Sprintf("failed to add tag %s to image manifest %s",
					tag, aws.StringValue(img.ImageManifest)))
			}
		}
	}
	return nil
}

// Remove removes the list of tags from ECR repository such that no image contains these
// tags
func (s *ecrProvider) Remove(tags ...string) error {
	for _, tag := range tags {
		getReq := &ecr.BatchGetImageInput{
			ImageIds: []*ecr.ImageIdentifier{
				{
					ImageTag: aws.String(tag),
				},
			},
			RegistryId:     aws.String(s.accountID),
			RepositoryName: aws.String(s.repoName),
		}

		getRes, err := s.ecr.BatchGetImage(getReq)
		if err != nil {
			s.stats.IncCount(fmt.Sprintf("registry.batchget.%s.failure", s.repoName))
			return errors.Wrap(err, fmt.Sprintf("failed to get images of tag %s", tag))
		}

		for _, img := range getRes.Images {

			delReq := &ecr.BatchDeleteImageInput{
				ImageIds: []*ecr.ImageIdentifier{
					{
						ImageTag:    aws.String(tag),
						ImageDigest: img.ImageId.ImageDigest,
					},
				},
				RegistryId:     aws.String(s.accountID),
				RepositoryName: aws.String(s.repoName),
			}

			_, err = s.ecr.BatchDeleteImage(delReq)
			if err != nil {
				s.stats.IncCount(fmt.Sprintf("registry.batchdelete.%s.failure", s.repoName))
				return errors.Wrap(err, fmt.Sprintf("failed to perform batch delete image by tag %s and digest %s",
					tag, aws.StringValue(img.ImageId.ImageDigest)))
			}
		}
	}
	return nil
}

// Get gets the list of tags to the image identified with version
func (s *ecrProvider) Get(version string) ([]string, error) {
	getReq := &ecr.DescribeImagesInput{
		ImageIds: []*ecr.ImageIdentifier{
			{
				ImageTag: aws.String(version),
			},
		},
		RegistryId:     aws.String(s.accountID),
		RepositoryName: aws.String(s.repoName),
	}

	getRes, err := s.ecr.DescribeImages(getReq)
	if err != nil {
		s.stats.IncCount(fmt.Sprintf("ecr.descimg.%s.failure", s.repoName))
		return nil, errors.Wrap(err, fmt.Sprintf("failed to get images of tag %s", version))
	}

	if len(getRes.ImageDetails) > 1 {
		return nil, errors.New("more than one image with version tag was found")
	}

	return aws.StringValueSlice(getRes.ImageDetails[0].ImageTags), nil

}

func (s *ecrProvider) currentVersion(img *ecr.ImageDetail) string {
	var tag string
	for _, t := range aws.StringValueSlice(img.ImageTags) {

		if s.vRegex.MatchString(t) {
			tag = t
			break
		}
	}
	return tag
}
