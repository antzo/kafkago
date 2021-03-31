package kafkago

import (
	"encoding/json"

	"github.com/google/uuid"
)

// Event represents a DomainEvent to be parsed and handled
type Event struct {
	AggregateId string
	Payload     string
	Name        string `json:"-"` // omit
	Version     int    `json:"-"` // omit
}

// NewEvent creates a new event, if the aggregateID is not provided,
// it will generate a fresh uuid
func NewEvent(
	aggregateID string,
	payload string,
	name string,
	version int,
) (Event, error) {
	_, err := uuid.Parse(aggregateID)
	if err != nil {
		return Event{}, err
	}

	return Event{
		AggregateId: aggregateID,
		Payload:     payload,
		Name:        name,
		Version:     version,
	}, nil
}

// Encode an event to JSON. Implements the sarama Encoder interface
func (e Event) Encode() ([]byte, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

// DecodeEvent will try to decode a JSON slice of bytes to a Event struct.
// It should not decode the Payload field.
func DecodeEvent(encodedEvent []byte, eventName string, version int) (Event, error) {
	event := Event{}
	err := json.Unmarshal(encodedEvent, &event)
	event.Version = version
	event.Name = eventName

	return event, err
}

// Length returns the length of the event payload. Must be implemented to
// complete the Sarama.Decode interface
func (e *Event) Length() int {
	return len(e.Payload)
}
