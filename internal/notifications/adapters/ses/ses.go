// Package ses (notifications adapter) implements ports.Mailer with Amazon SES v2.
//
// HEXAGONAL BOUNDARY: adapter. Its main job is ERROR CLASSIFICATION, because the
// worker must treat "try again later" and "never going to work" differently:
//
//	TooManyRequestsException, LimitExceededException   throttled   -> ErrMailThrottled (retry via SQS)
//	MessageRejected (bad/unverified address, sandbox)  permanent   -> ErrMailRejected (record failed, ack)
//	AccountSuspended, SendingPaused, MailFromDomainNotVerified,
//	  NotFound (identity/config set), network errors   operational -> plain error (retry, then DLQ,
//	                                                    which makes the problem visible)
//
// IAM: ses:SendEmail on the verified identity ARN (arn:aws:ses:<region>:<account-id>:identity/<from>)
// and, when a configuration set is used, on arn:aws:ses:<region>:<account-id>:configuration-set/<name>.
// In the SES SANDBOX only verified recipients can receive mail; the mailbox simulator
// addresses (success@simulator.amazonses.com, bounce@..., complaint@...) always work.
package ses

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
)

// API is the subset of *sesv2.Client used.
type API interface {
	SendEmail(ctx context.Context, in *sesv2.SendEmailInput, opts ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error)
}

// Mailer implements ports.Mailer.
type Mailer struct {
	Client           API
	From             string // SES_FROM_ADDRESS (verified identity)
	ConfigurationSet string // SES_CONFIGURATION_SET (optional; bounce/complaint event publishing)
}

var _ ports.Mailer = (*Mailer)(nil)

// Send sends one message and returns the SES message id.
func (m *Mailer) Send(ctx context.Context, to string, e domain.Email) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	in := &sesv2.SendEmailInput{
		FromEmailAddress: aws.String(m.From),
		Destination:      &types.Destination{ToAddresses: []string{to}},
		Content: &types.EmailContent{Simple: &types.Message{
			Subject: &types.Content{Data: aws.String(e.Subject), Charset: aws.String("UTF-8")},
			Body: &types.Body{
				Text: &types.Content{Data: aws.String(e.Text), Charset: aws.String("UTF-8")},
				Html: &types.Content{Data: aws.String(e.HTML), Charset: aws.String("UTF-8")},
			},
		}},
	}
	if m.ConfigurationSet != "" {
		in.ConfigurationSetName = aws.String(m.ConfigurationSet)
	}
	out, err := m.Client.SendEmail(ctx, in)
	if err != nil {
		return "", classify(err)
	}
	return aws.ToString(out.MessageId), nil
}

func classify(err error) error {
	var tooMany *types.TooManyRequestsException
	var limit *types.LimitExceededException
	var rejected *types.MessageRejected
	switch {
	case errors.As(err, &tooMany), errors.As(err, &limit):
		return fmt.Errorf("%w: %w", domain.ErrMailThrottled, err)
	case errors.As(err, &rejected):
		return fmt.Errorf("%w: %w", domain.ErrMailRejected, err)
	}
	return err
}
