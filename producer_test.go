package kafkago

import (
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestPublish(t *testing.T) {
	if !*integrationKafkaTest {
		t.Skip("kafka broker disabled")
	}

	testCases := []struct {
		desc      string
		eventName string
		topics    []string
		payload   string
	}{
		{
			desc:      "Publish an operation.built event",
			topics:    []string{"operations"},
			eventName: "operation.built",
			payload:   `{"OperatorId":"ea514e23-7d4f-4aae-9ebf-998d3cd6e0f4"}`,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			p := NewProducer(NewKafkaConfiguration(
				"localhost:9092",
				"test-group",
				"test-client-id",
				false,
			))

			defer func() {
				if err := p.Writer.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			evt, err := NewEvent(
				uuid.New().String(),
				tC.payload,
				"operation.built",
				1,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Publish(evt); err != nil {
				t.Fatal(err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range p.Writer.Successes() {
					break
				}
			}()
			wg.Wait()
		})
	}
}
