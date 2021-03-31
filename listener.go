package kafkago

// Listener provides an interface to process an event.
type Listener interface {
	Process(e Event) error
}

// EventListenerMap maintains a relation between an EventName and his listeners.
// When an Event is decoded, then the EventListenerMap will execute all the
// listeners associated
type EventListenerMap map[string][]Listener
