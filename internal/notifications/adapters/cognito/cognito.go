// Package cognito (notifications adapter) implements ports.Directory: it looks up
// a user's e-mail address in the Cognito user pool.
//
// HEXAGONAL BOUNDARY: adapter.
//
// Why a lookup instead of putting the address in events: Cognito ACCESS tokens do
// not contain the e-mail, and events must carry ids only (an address on a bus
// would be readable by every rule target and archive). The worker therefore asks
// Cognito at send time, with the user's `sub` as the username.
//
// IAM: cognito-idp:AdminGetUser on arn:aws:cognito-idp:<region>:<account-id>:userpool/<COGNITO_USER_POOL_ID>.
// UNVERIFIED: that AdminGetUser accepts the `sub` as Username for this pool's
// configuration (documented as "the username or alias"; with email-as-username
// pools the real username is a UUID that equals `sub`).
// Failure shapes: UserNotFoundException (permanent), NotAuthorizedException /
// AccessDeniedException (IAM), TooManyRequestsException (throttling, retried).
package cognito

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
)

// API is the subset of *cognitoidentityprovider.Client used.
type API interface {
	AdminGetUser(ctx context.Context, in *cognitoidentityprovider.AdminGetUserInput, opts ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.AdminGetUserOutput, error)
}

// Directory implements ports.Directory.
type Directory struct {
	Client     API
	UserPoolID string
}

var _ ports.Directory = (*Directory)(nil)

// EmailOf returns the e-mail attribute of the user.
func (d *Directory) EmailOf(ctx context.Context, userID string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := d.Client.AdminGetUser(ctx, &cognitoidentityprovider.AdminGetUserInput{UserPoolId: aws.String(d.UserPoolID), Username: aws.String(userID)})
	var nf *types.UserNotFoundException
	if errors.As(err, &nf) {
		return "", domain.ErrUserNotFound
	}
	if err != nil {
		return "", err
	}
	for _, a := range out.UserAttributes {
		if aws.ToString(a.Name) == "email" && aws.ToString(a.Value) != "" {
			return aws.ToString(a.Value), nil
		}
	}
	return "", domain.ErrNoEmail
}
