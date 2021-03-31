package kafkago

import (
	"flag"
	"reflect"
	"testing"
)

var integrationKafkaTest = flag.Bool("integration-kafka-test", false, "If true, it will run tests agains a real kafka broker")

type MockListener struct{}

func (m MockListener) Process(e Event) error {
	return nil
}

func setUp() []Listener {
	var listeners []Listener
	listeners = append(listeners, MockListener{})
	return listeners
}

func TestCreateConsumer(t *testing.T) {
	if !*integrationKafkaTest {
		t.Skip("kafka broker disabled")
	}
	testCases := []struct {
		desc string
		want []string
	}{
		{
			desc: "eventListenerMap will create a list of topics",
			want: []string{"test.event", "second.test.event"},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			listeners := setUp()
			eM := make(EventListenerMap)
			for _, e := range tC.want {
				eM[e] = listeners
			}
			config := NewKafkaConfiguration(
				"localhost:9092",
				"test-group",
				"test-client-id",
				false,
			)
			got := NewConsumer(eM, config, nil)

			if !reflect.DeepEqual(got.Topics, tC.want) {
				t.Errorf("consumer event map error: got topics: %s and want topics %s\n", got.Topics, tC.want)
			}
		})
	}
}
