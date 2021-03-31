package kafkago

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

var (
	kafkaHeaderEventName = []byte("X-Domain-Event")
	kafkaHeaderVersion   = []byte("X-Version")
	kafkaHeaderRetry     = []byte("X-Retry")

	errIncorrectHeaderVersion = errors.New("Incorrect kafka message version")
	errNoHeaderVersion        = errors.New("No X-Version header present in the message")
)

const (
	deadLetterSuffix = ".dead_letter"
	retryTopicSuffix = ".retry"
)

// ErrConsumerFailedToStart is throw when the producer fails to start
type ErrConsumerFailedToStart struct {
	brokers string
	err     error
}

func (e *ErrConsumerFailedToStart) Error() string {
	return "consumer: failed to start with brokers " + e.brokers + " and error: " + e.err.Error()
}

// Consumer defines kafka consumer implementation
type Consumer struct {
	Topics   []string
	Ready    chan bool
	EventMap EventListenerMap
	config   *KafkaConfiguration

	reader   sarama.ConsumerGroup
	producer sarama.AsyncProducer
}

// NewConsumer creates a consumer group for the given topic
func NewConsumer(
	eM EventListenerMap,
	c KafkaConfiguration,
	p sarama.AsyncProducer,
) *Consumer {
	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		log.Panic().
			Msg(err.Error())
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.ClientID = c.ClientID

	client, err := sarama.NewConsumerGroup(
		strings.Split(c.Brokers, ","),
		c.ConsumerGroup,
		config,
	)

	if err != nil {
		errConsumerFail := ErrConsumerFailedToStart{
			brokers: c.Brokers,
			err:     err,
		}
		log.Panic().
			Msgf(errConsumerFail.Error())
	}

	var topics []string
	for topic := range eM {
		topics = append(topics, topic)
	}

	// Creates a new async producer to send messages to retry/dead-letter topics in case of failure
	producer := p
	if p == nil {
		producer, err = sarama.NewAsyncProducer(strings.Split(c.Brokers, ","), newSaramaProducerConfig(c))
		if err != nil {
			errProducerFail := ErrProducerFailedToStart{
				brokers: c.Brokers,
				err:     err,
			}
			log.Panic().Msg(errProducerFail.Error())
		}

		// Track producer errors
		go func() {
			for err := range producer.Errors() {
				log.Error().Msgf("producer: error: %s", err)
			}
		}()
	}

	return &Consumer{
		reader:   client,
		Topics:   topics,
		Ready:    make(chan bool, 1),
		EventMap: eM,
		producer: producer,
		config:   &c,
	}
}

// Setup the consumer group
func (c Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	close(c.Ready)
	return nil
}

// Cleanup the consumer group
func (c Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Consume will start to listening for messages from a group
func (c *Consumer) Consume(ctx context.Context) {
	for {
		if err := c.reader.Consume(ctx, c.Topics, c); err != nil {
			log.Panic().
				Msgf("Error from consumer: %v", err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			log.Debug().
				Msgf("consumer: context err: %s", ctx.Err().Error())
			return
		}
		// Mark as ready, do not put something into the channel so it can be already closed
		c.Ready = make(chan bool, 1)
	}
}

// ConsumeClaim sarama implementation to consume from a group
func (c Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	// Please note, that once a rebalance is triggered, sessions must be completed within
	// Config.Consumer.Group.Rebalance.Timeout. This means that ConsumeClaim() functions must exit
	// as quickly as possible to allow time for Cleanup() and the final offset commit. If the timeout
	// is exceeded, the consumer will be removed from the group by Kafka, which will cause offset
	// commit failures.
	for msg := range claim.Messages() {
		log.Debug().
			Msgf(
				"consumer: message received: %s from topic: %s with key: %s",
				msg.Value,
				msg.Topic,
				msg.Key,
			)
		var eventName string
		var version int
		for _, header := range msg.Headers {
			if bytes.Compare(header.Key, kafkaHeaderEventName) == 0 {
				eventName = string(header.Value)
				continue
			}

			v, err := eventVersion(header)
			if err != nil {
				continue
			}
			version = v
		}

		eventListeners, ok := c.EventMap[eventName]
		if !ok {
			log.Debug().Msgf("consumer: event %s filtered and not processed", eventName)
			session.MarkMessage(msg, "")
			continue
		}

		event, err := DecodeEvent(msg.Value, eventName, version)
		if err != nil {
			log.Debug().Msgf("consumer: decode error: %s", err)
			// Mark message as readed so we cannot even unserialize it
			session.MarkMessage(msg, "")
			// In this case we can think about how to recover from this error automatically.
			// I think that this one it's very tricky and only happen when we have a bug in our code
			continue
		}

		for _, l := range eventListeners {
			// TODO: Add context to processor, to cancelWithTimeout
			if err := l.Process(event); err != nil {
				log.Error().Msgf("consumer: processor: error: %s", err)
				c.sendEventToRetryTopic(event, msg.Headers)
				continue
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

// sendEventToDeadLetter will produce a message to a kafka dead-letter
func (c *Consumer) sendEventToDeadLetter(e Event) {
	log.Debug().Msgf("consumer: sending message %s to the dead-letter topic", e.Name)
	c.producer.Input() <- &sarama.ProducerMessage{
		Topic:   e.Name + deadLetterSuffix,
		Key:     sarama.StringEncoder(e.AggregateId),
		Value:   &e,
		Headers: kafkaHeaders(e),
	}
}

// sendEventToRetryTopic will produce a message to a topic for retries.
// If the X-Retry header is present, it will add 1 to the number of
// retries. And if the number of retries is equals the maximum amount of
// retries it will be sent to the dead-letter topic instead
func (c *Consumer) sendEventToRetryTopic(e Event, headers []*sarama.RecordHeader) {
	var numOfRetries int = 1
	var err error

	// Add one to X-Retry header, if present
	for _, header := range headers {
		if bytes.Compare(header.Key, kafkaHeaderRetry) == 0 {
			val := string(header.Value)
			numOfRetries, err = strconv.Atoi(val)
			if err != nil {
				log.Error().
					Msgf("consumer: error sending the event to the retry topic. unable to parse %s header with value %s",
						kafkaHeaderRetry, val)
				return
			}
			numOfRetries++
			header.Value = []byte(strconv.Itoa(numOfRetries))
			break
		}
	}

	// If numOfRetries is equals or greater than MaxNumberOfRetries (default: 3)
	// send to the dead-letter topic
	if numOfRetries == c.config.MaxNumberOfRetries {
		c.sendEventToDeadLetter(e)
		return
	}

	// First retry
	if numOfRetries == 1 {
		headers = append(headers, &sarama.RecordHeader{
			Key:   kafkaHeaderRetry,
			Value: []byte(string("1")),
		})
	}

	borrowed := []sarama.RecordHeader{}
	for _, v := range headers {
		borrowed = append(borrowed, *v)
	}

	log.Debug().Msgf("consumer: sending message %s to the retry topic with %d retries", e.Name, numOfRetries)
	c.producer.Input() <- &sarama.ProducerMessage{
		Topic:   e.Name + retryTopicSuffix,
		Key:     sarama.StringEncoder(e.AggregateId),
		Value:   &e,
		Headers: borrowed,
	}
}

// Close the consumer tcp connection
func (c *Consumer) Close() error {
	return c.reader.Close()
}

func eventVersion(h *sarama.RecordHeader) (int, error) {
	if bytes.Compare(h.Key, kafkaHeaderVersion) == 0 {
		eventVersion, err := strconv.ParseInt(string(h.Value), 10, 8)
		if err != nil {
			log.Error().
				Msgf("consumer: incorrect message version format: %s", h.Value)
			return 0, errIncorrectHeaderVersion
		}
		return int(eventVersion), nil
	}
	return 0, errNoHeaderVersion
}
