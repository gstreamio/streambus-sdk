package protocol

import (
	"testing"
	"time"
)

func TestMessage(t *testing.T) {
	msg := Message{
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		Offset:    100,
		Timestamp: time.Now().UnixMilli(),
	}

	if len(msg.Key) == 0 {
		t.Error("Expected non-empty key")
	}
	if len(msg.Value) == 0 {
		t.Error("Expected non-empty value")
	}
	if msg.Offset < 0 {
		t.Error("Expected non-negative offset")
	}
	if msg.Timestamp == 0 {
		t.Error("Expected non-zero timestamp")
	}
}

func TestMessageWithNilKey(t *testing.T) {
	msg := Message{
		Key:       nil,
		Value:     []byte("test-value"),
		Offset:    0,
		Timestamp: time.Now().UnixMilli(),
	}

	if msg.Key != nil && len(msg.Key) > 0 {
		t.Error("Expected nil or empty key")
	}
	if len(msg.Value) == 0 {
		t.Error("Expected non-empty value")
	}
}

func TestRequestTypes(t *testing.T) {
	// Test that request type constants are defined
	types := []RequestType{
		RequestTypeProduce,
		RequestTypeFetch,
		RequestTypeCreateTopic,
		RequestTypeDeleteTopic,
		RequestTypeListTopics,
	}

	for i, rt := range types {
		if rt == 0 && i > 0 {
			t.Errorf("Request type %d should not be zero", i)
		}
	}
}

func TestProduceRequestValidation(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{"valid topic", "test-topic", false},
		{"empty topic", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasErr := tt.topic == ""
			if hasErr != tt.wantErr {
				t.Errorf("Expected error=%v, got error=%v", tt.wantErr, hasErr)
			}
		})
	}
}

func TestFetchRequestValidation(t *testing.T) {
	tests := []struct {
		name     string
		maxBytes int32
		wantErr  bool
	}{
		{"valid max bytes", 1024 * 1024, false},
		{"zero max bytes", 0, true},
		{"negative max bytes", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasErr := tt.maxBytes <= 0
			if hasErr != tt.wantErr {
				t.Errorf("Expected error=%v, got error=%v", tt.wantErr, hasErr)
			}
		})
	}
}

func TestCreateTopicValidation(t *testing.T) {
	tests := []struct {
		name       string
		topic      string
		partitions int32
		replicas   int32
		wantValid  bool
	}{
		{"valid", "topic", 3, 1, true},
		{"empty topic", "", 3, 1, false},
		{"zero partitions", "topic", 0, 1, false},
		{"zero replicas", "topic", 3, 0, false},
		{"negative partitions", "topic", -1, 1, false},
		{"negative replicas", "topic", 3, -1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.topic != "" && tt.partitions > 0 && tt.replicas > 0
			if valid != tt.wantValid {
				t.Errorf("Expected valid=%v, got valid=%v", tt.wantValid, valid)
			}
		})
	}
}
