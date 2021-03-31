package kafkago

import (
	"testing"
)

func TestEncodeEvent(t *testing.T) {
	testCases := []struct {
		desc        string
		aggregateID string
		payload     string
		name        string
		version     int
		want        string
	}{
		{
			desc:        "test.event encode event",
			aggregateID: "201fe952-f796-4360-8e1d-77807ea2a5c3",
			payload:     `{"test":"1234"}`,
			name:        "test.event",
			version:     1,
			want:        `{"AggregateId":"201fe952-f796-4360-8e1d-77807ea2a5c3","Payload":"{\"test\":\"1234\"}"}`,
		},
	}

	for _, tC := range testCases {
		evt, err := NewEvent(
			tC.aggregateID,
			tC.payload,
			tC.name,
			tC.version,
		)
		if err != nil {
			t.Fatal(err)
		}
		got, err := evt.Encode()
		if err != nil {
			t.Error(err)
		}

		if string(got) != tC.want {
			t.Errorf("event: encode: got %s but want %s", got, tC.want)
		}
	}
}

func TestDecodeEvent(t *testing.T) {
	testCases := []struct {
		desc    string
		payload []byte
		version int
		name    string
	}{
		{
			desc:    "test.event decode event",
			payload: []byte(`{"AggregateId":"177d0e6b-45f1-4054-b4f0-bf7ca5d5ed21","Payload":"{\"test\":1234}"}`),
			version: 1,
			name:    "test.event",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			want, _ := NewEvent(
				"177d0e6b-45f1-4054-b4f0-bf7ca5d5ed21",
				"{\"test\":1234}",
				tC.name,
				1,
			)
			got, err := DecodeEvent(tC.payload, tC.name, tC.version)
			if got != want {
				t.Errorf("event: decode: got \"%v\" want \"%v\": error: %s", got, want, err)
			}
		})
	}
}

func TestInvalidAggregateIDShouldReturnError(t *testing.T) {
	_, err := NewEvent(
		"invalid uuid",
		"{\"test\":1234}",
		"test.event",
		1,
	)

	if err == nil {
		t.Errorf("event: aggregate_id: %s is not a valid uuid", "invalid uuid")
	}
}
