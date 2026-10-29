package ses

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
)

type fake struct {
	err error
	in  *sesv2.SendEmailInput
}

func (f *fake) SendEmail(_ context.Context, in *sesv2.SendEmailInput, _ ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error) {
	f.in = in
	if f.err != nil {
		return nil, f.err
	}
	return &sesv2.SendEmailOutput{MessageId: aws.String("m-1")}, nil
}

func TestErrorClassification(t *testing.T) {
	cases := []struct {
		err  error
		want error
	}{
		{&types.TooManyRequestsException{}, domain.ErrMailThrottled},
		{&types.LimitExceededException{}, domain.ErrMailThrottled},
		{&types.MessageRejected{}, domain.ErrMailRejected},
	}
	for _, c := range cases {
		_, err := (&Mailer{Client: &fake{err: c.err}, From: "from@example.com"}).Send(context.Background(), "to@example.com", domain.Email{})
		if !errors.Is(err, c.want) {
			t.Errorf("%T: got %v, want %v", c.err, err, c.want)
		}
	}
	// operational errors keep their identity (retry, then DLQ makes them visible)
	other := &types.AccountSuspendedException{}
	if _, err := (&Mailer{Client: &fake{err: other}}).Send(context.Background(), "a@example.com", domain.Email{}); errors.Is(err, domain.ErrMailRejected) || errors.Is(err, domain.ErrMailThrottled) || err == nil {
		t.Errorf("suspended account misclassified: %v", err)
	}
}

func TestSendBuildsTheRequest(t *testing.T) {
	f := &fake{}
	m := &Mailer{Client: f, From: "tickets@example.com", ConfigurationSet: "gatepass"}
	id, err := m.Send(context.Background(), "success@simulator.amazonses.com", domain.Email{Subject: "S", Text: "T", HTML: "<p>H</p>"})
	if err != nil || id != "m-1" {
		t.Fatalf("%v %s", err, id)
	}
	if aws.ToString(f.in.FromEmailAddress) != "tickets@example.com" || aws.ToString(f.in.ConfigurationSetName) != "gatepass" ||
		f.in.Destination.ToAddresses[0] != "success@simulator.amazonses.com" || f.in.Content.Simple.Body.Html == nil {
		t.Fatalf("%+v", f.in)
	}
}
