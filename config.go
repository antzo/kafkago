package kafkago

import (
	"time"
)

// KafkaConfiguration defines the configurable properties of kafka
type KafkaConfiguration struct {
	// Brokers contains the comma separated list of kafka hosts
	Brokers string

	// ConsumerGroup name
	ConsumerGroup string

	// ClientID to be used in kafka
	ClientID string

	// WriteTimeout for write operation performed by the Writer
	WriteTimeout time.Duration

	// ReadTimeout for read operations performed by the Reader
	ReadTimeout time.Duration

	// Debug will output debugging messages. Default is false
	Debug bool

	// MaxNumberOfRetries defines the max number of X-Retry header. If the number of retries is
	// equals to this number the event will be sent to the dead-letter topic instead of the retry
	// topic
	MaxNumberOfRetries int
}

// NewKafkaConfiguration generated a default configuration for kafka
func NewKafkaConfiguration(
	brokers string,
	consumerGroup string,
	clientId string,
	debug bool,
) KafkaConfiguration {
	return KafkaConfiguration{
		Brokers:            brokers,
		ConsumerGroup:      consumerGroup,
		ClientID:           clientId,
		Debug:              debug,
		WriteTimeout:       10 * time.Second,
		ReadTimeout:        10 * time.Second,
		MaxNumberOfRetries: 3,
	}
}
