package kafkago

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

// Producer defines an event dispatcher implementation for kafka
type Producer struct {
	Writer sarama.AsyncProducer
}

var (
	errEventWithoutTopicAssociated = errors.New("producer: publish: event has no topic associated")
)

// ErrProducerFailedToStart is throw when the producer fails to start
type ErrProducerFailedToStart struct {
	brokers string
	err     error
}

func (e *ErrProducerFailedToStart) Error() string {
	return "producer: failed to start with brokers " + e.brokers + " and error: " + e.err.Error()
}

// NewProducer creates a async producer with a default configuration
func NewProducer(c KafkaConfiguration) *Producer {
	producer, err := sarama.NewAsyncProducer(strings.Split(c.Brokers, ","), newSaramaProducerConfig(c))
	if err != nil {
		errProducerFail := ErrProducerFailedToStart{
			brokers: c.Brokers,
			err:     err,
		}
		log.Panic().Msg(errProducerFail.Error())
	}

	go func() {
		for err := range producer.Errors() {
			log.Error().Msgf("producer: error: %s", err)
		}
	}()

	return &Producer{Writer: producer}
}

func newSaramaProducerConfig(c KafkaConfiguration) *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Return.Successes = true
	config.ClientID = c.ClientID
	return config
}

// Publish an event to the configured topics
func (d *Producer) Publish(e Event) error {
	d.Writer.Input() <- &sarama.ProducerMessage{
		Topic:   e.Name,
		Key:     sarama.StringEncoder(e.AggregateId),
		Value:   &e,
		Headers: kafkaHeaders(e),
	}
	log.Debug().Msgf("producer: publishing event to topic %s", e.Name)
	return nil
}

func kafkaHeaders(e Event) []sarama.RecordHeader {
	return []sarama.RecordHeader{
		{
			Key:   kafkaHeaderEventName,
			Value: []byte(e.Name),
		},
		{
			Key:   kafkaHeaderVersion,
			Value: []byte(strconv.Itoa(e.Version)),
		},
	}
}
