package client

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if len(config.Brokers) == 0 {
		t.Error("Expected default brokers to be set")
	}
	if config.ConnectTimeout == 0 {
		t.Error("Expected default connect timeout to be set")
	}
	if config.RequestTimeout == 0 {
		t.Error("Expected default request timeout to be set")
	}
	if config.MaxConnectionsPerBroker == 0 {
		t.Error("Expected default max connections per broker to be set")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Brokers:                 []string{"localhost:9092"},
				ConnectTimeout:          10 * time.Second,
				RequestTimeout:          30 * time.Second,
				MaxConnectionsPerBroker: 5,
			},
			wantErr: false,
		},
		{
			name: "empty brokers",
			config: &Config{
				Brokers:        []string{},
				ConnectTimeout: 10 * time.Second,
				RequestTimeout: 30 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "nil brokers",
			config: &Config{
				Brokers:        nil,
				ConnectTimeout: 10 * time.Second,
				RequestTimeout: 30 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "zero connect timeout",
			config: &Config{
				Brokers:        []string{"localhost:9092"},
				ConnectTimeout: 0,
				RequestTimeout: 30 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "zero request timeout",
			config: &Config{
				Brokers:        []string{"localhost:9092"},
				ConnectTimeout: 10 * time.Second,
				RequestTimeout: 0,
			},
			wantErr: true,
		},
		{
			name: "negative max retries",
			config: &Config{
				Brokers:        []string{"localhost:9092"},
				ConnectTimeout: 10 * time.Second,
				RequestTimeout: 30 * time.Second,
				MaxRetries:     -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTLSConfig(t *testing.T) {
	tests := []struct {
		name      string
		tlsConfig *TLSConfig
		wantErr   bool
	}{
		{
			name: "valid TLS config",
			tlsConfig: &TLSConfig{
				Enabled:    true,
				CAFile:     "/path/to/ca.crt",
				ServerName: "localhost",
			},
			wantErr: false,
		},
		{
			name: "TLS disabled",
			tlsConfig: &TLSConfig{
				Enabled: false,
			},
			wantErr: false,
		},
		{
			name: "TLS enabled without CA file",
			tlsConfig: &TLSConfig{
				Enabled:            true,
				CAFile:             "",
				InsecureSkipVerify: true,
			},
			wantErr: false,
		},
		{
			name: "mTLS config",
			tlsConfig: &TLSConfig{
				Enabled:    true,
				CAFile:     "/path/to/ca.crt",
				CertFile:   "/path/to/client.crt",
				KeyFile:    "/path/to/client.key",
				ServerName: "localhost",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.tlsConfig.Enabled && !tt.tlsConfig.InsecureSkipVerify && tt.tlsConfig.CAFile == "" {
				t.Error("Expected CA file when TLS is enabled and InsecureSkipVerify is false")
			}
			if (tt.tlsConfig.CertFile != "") != (tt.tlsConfig.KeyFile != "") {
				t.Error("CertFile and KeyFile must both be set or both be empty")
			}
		})
	}
}

func TestSASLConfig(t *testing.T) {
	tests := []struct {
		name       string
		saslConfig *SASLConfig
		wantValid  bool
	}{
		{
			name: "valid SASL config",
			saslConfig: &SASLConfig{
				Enabled:   true,
				Mechanism: "SCRAM-SHA-256",
				Username:  "user",
				Password:  "pass",
			},
			wantValid: true,
		},
		{
			name: "SASL disabled",
			saslConfig: &SASLConfig{
				Enabled: false,
			},
			wantValid: true,
		},
		{
			name: "SASL enabled without username",
			saslConfig: &SASLConfig{
				Enabled:   true,
				Mechanism: "SCRAM-SHA-256",
				Username:  "",
				Password:  "pass",
			},
			wantValid: false,
		},
		{
			name: "SASL enabled without password",
			saslConfig: &SASLConfig{
				Enabled:   true,
				Mechanism: "SCRAM-SHA-256",
				Username:  "user",
				Password:  "",
			},
			wantValid: false,
		},
		{
			name: "SASL enabled without mechanism",
			saslConfig: &SASLConfig{
				Enabled:   true,
				Mechanism: "",
				Username:  "user",
				Password:  "pass",
			},
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := !tt.saslConfig.Enabled ||
				(tt.saslConfig.Mechanism != "" &&
					tt.saslConfig.Username != "" &&
					tt.saslConfig.Password != "")

			if valid != tt.wantValid {
				t.Errorf("Expected valid=%v, got valid=%v", tt.wantValid, valid)
			}
		})
	}
}

func TestProducerConfig(t *testing.T) {
	config := &ProducerConfig{
		RequireAck:   true,
		BatchTimeout: 5 * time.Second,
		Compression:  "none",
	}

	if !config.RequireAck {
		t.Error("Expected RequireAck to be true")
	}
	if config.BatchTimeout <= 0 {
		t.Error("Expected positive batch timeout")
	}
}

func TestConsumerConfig(t *testing.T) {
	config := &ConsumerConfig{
		GroupID:       "test-group",
		StartOffset:   0,
		MaxFetchBytes: 1024 * 1024,
	}

	if config.GroupID == "" {
		t.Error("Expected non-empty group ID")
	}
	if config.MaxFetchBytes <= 0 {
		t.Error("Expected positive max fetch bytes")
	}
}

func TestGroupConsumerConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    *GroupConsumerConfig
		wantValid bool
	}{
		{
			name: "valid config",
			config: &GroupConsumerConfig{
				GroupID: "test-group",
				Topics:  []string{"topic1", "topic2"},
			},
			wantValid: true,
		},
		{
			name: "empty group ID",
			config: &GroupConsumerConfig{
				GroupID: "",
				Topics:  []string{"topic1"},
			},
			wantValid: false,
		},
		{
			name: "empty topics",
			config: &GroupConsumerConfig{
				GroupID: "test-group",
				Topics:  []string{},
			},
			wantValid: false,
		},
		{
			name: "nil topics",
			config: &GroupConsumerConfig{
				GroupID: "test-group",
				Topics:  nil,
			},
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.config.GroupID != "" && len(tt.config.Topics) > 0
			if valid != tt.wantValid {
				t.Errorf("Expected valid=%v, got valid=%v", tt.wantValid, valid)
			}
		})
	}
}
