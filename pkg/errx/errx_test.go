package errx_test

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/palemoky/oppenheimer/pkg/errx"
)

func TestNewCarriesCodeReasonAndMessage(t *testing.T) {
	err := errx.New(codes.FailedPrecondition, errx.ReasonInsufficientStock, "sku %d has %d left", 7, 0)

	if got := errx.Code(err); got != codes.FailedPrecondition {
		t.Errorf("Code = %v, want FailedPrecondition", got)
	}
	if got := errx.Reason(err); got != errx.ReasonInsufficientStock {
		t.Errorf("Reason = %q, want %q", got, errx.ReasonInsufficientStock)
	}
	if !errx.Is(err, errx.ReasonInsufficientStock) || errx.Is(err, errx.ReasonSKUNotFound) {
		t.Error("Is did not match exactly the carried reason")
	}
	if got := status.Convert(err).Message(); got != "sku 7 has 0 left" {
		t.Errorf("message = %q", got)
	}
}

func TestReasonSurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("reserve: %w", errx.New(codes.NotFound, errx.ReasonSKUNotFound, "no such sku"))
	if !errx.Is(err, errx.ReasonSKUNotFound) {
		t.Errorf("Reason lost after wrapping: %v", err)
	}
}

func TestReasonOfForeignErrors(t *testing.T) {
	for _, err := range []error{nil, errors.New("plain"), status.Error(codes.Internal, "no details")} {
		if got := errx.Reason(err); got != "" {
			t.Errorf("Reason(%v) = %q, want empty", err, got)
		}
		if errx.Is(err, "") {
			t.Errorf("Is(%v, \"\") = true, want false", err)
		}
	}
}

func TestDegraded(t *testing.T) {
	err := errx.Degraded(errx.ReasonStockGateUnavailable, "stock gate unavailable")
	// 不能是 UNAVAILABLE：调用方的边车会把它算成这个服务的失败（见 Degraded 的注释）。
	if got := errx.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("code = %s, want FailedPrecondition", got)
	}
	if !errx.IsDegraded(err) || !errx.IsDegraded(fmt.Errorf("wrapped: %w", err)) {
		t.Fatal("IsDegraded = false for a degraded error")
	}
	for _, other := range []error{
		nil,
		errx.New(codes.FailedPrecondition, errx.ReasonInsufficientStock, "no stock"),
		errx.New(codes.ResourceExhausted, errx.ReasonBacklogged, "busy"),
		status.Error(codes.Unavailable, "down"),
		errors.New("plain"),
	} {
		if errx.IsDegraded(other) {
			t.Errorf("errx.IsDegraded(%v) = true, want false", other)
		}
	}
}
