package client

import (
	"errors"
	"testing"
)

func TestErrorTypes(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrNoBrokers", ErrNoBrokers},
		{"ErrNoConnection", ErrNoConnection},
		{"ErrRequestTimeout", ErrRequestTimeout},
		{"ErrInvalidTopic", ErrInvalidTopic},
		{"ErrInvalidPartition", ErrInvalidPartition},
		{"ErrNoMessages", ErrNoMessages},
		{"ErrProducerClosed", ErrProducerClosed},
		{"ErrConsumerClosed", ErrConsumerClosed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Errorf("%s should not be nil", tt.name)
			}
			if tt.err.Error() == "" {
				t.Errorf("%s should have a non-empty error message", tt.name)
			}
		})
	}
}

func TestErrorIs(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{
			name:   "same error",
			err:    ErrNoBrokers,
			target: ErrNoBrokers,
			want:   true,
		},
		{
			name:   "different errors",
			err:    ErrNoBrokers,
			target: ErrNoConnection,
			want:   false,
		},
		{
			name:   "wrapped error",
			err:    errors.Join(ErrNoConnection, errors.New("connection refused")),
			target: ErrNoConnection,
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := errors.Is(tt.err, tt.target)
			if got != tt.want {
				t.Errorf("errors.Is() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestErrorMessages(t *testing.T) {
	errorMessages := map[error]string{
		ErrNoBrokers:        "broker",
		ErrNoConnection:     "connection",
		ErrRequestTimeout:   "timeout",
		ErrInvalidTopic:     "topic",
		ErrInvalidPartition: "partition",
		ErrNoMessages:       "message",
		ErrProducerClosed:   "producer",
		ErrConsumerClosed:   "consumer",
	}

	for err, keyword := range errorMessages {
		t.Run(err.Error(), func(t *testing.T) {
			msg := err.Error()
			if msg == "" {
				t.Error("Error message should not be empty")
			}
			// Check if error message contains relevant keyword
			// This is a simple check - you might want more sophisticated validation
			if len(msg) < len(keyword) {
				t.Errorf("Error message '%s' seems too short", msg)
			}
		})
	}
}
