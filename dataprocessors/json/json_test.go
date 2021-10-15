package json

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataprocessors/json"))

func TestJson(t *testing.T) {
	data, err := os.ReadFile("../../test/assets/data/json/mock_array.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	string_valid_value, err := os.ReadFile("../../test/assets/data/json/observation_string_valid_value.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	string_invalid_value, err := os.ReadFile("../../test/assets/data/json/observation_string_invalid_value.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Run("Init()", testInitFunc())
	t.Run("GetObservations()", testGetObservationsFunc(data))
	t.Run("GetObservations() -- with selected measurements", testGetObservationsSelectedMeasurementsFunc(data))
	t.Run("GetObservations() -- with a string value for some data points", testGetObservationsFunc(string_valid_value))
	t.Run("GetObservations() -- with an invalid string value for some data points", testGetObservationsInvalidStringFunc(string_invalid_value))
	t.Run("GetObservations() called before Init()", testGetObservationsNoInitFunc())
	t.Run("GetObservations() called twice", testGetObservationsTwiceFunc(data))
	t.Run("GetObservations() updated with same data", testGetObservationsSameDataFunc(data))
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewJsonProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params, nil, nil)
		assert.NoError(t, err)
	}
}

// Tests "GetObservations()"
func testGetObservationsFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"eventId": "eventId",
			"height":  "height",
			"rating":  "rating",
			"speed":   "speed",
			"target":  "target",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, measurements, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		if err != nil {
			t.Error(err)
			return
		}

		expectedFirstObservation := observations.Observation{
			Time: 1605312000,
			Measurements: map[string]float64{
				"eventId": 806.42,
				"height":  29,
				"rating":  86,
				"speed":   15,
				"target":  42,
			},
			Tags: []string{"testTagA", "testTagB"},
		}
		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

		snapshotter.SnapshotT(t, actualObservations)
	}
}

// Tests "GetObservations()" with selected measurements
func testGetObservationsSelectedMeasurementsFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"eventId": "eventId",
			"rating":  "rating",
			"target":  "target",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, measurements, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		if err != nil {
			t.Error(err)
			return
		}

		expectedFirstObservation := observations.Observation{
			Time: 1605312000,
			Measurements: map[string]float64{
				"eventId": 806.42,
				"rating":  86,
				"target":  42,
			},
			Tags: []string{"testTagA", "testTagB"},
		}
		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

		snapshotter.SnapshotT(t, actualObservations)
	}
}

// Tests "GetObservations()" when given an invalid string data point
func testGetObservationsInvalidStringFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"eventId": "eventId",
			"height":  "height",
			"rating":  "rating",
			"speed":   "speed",
			"target":  "target",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, measurements, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		_, err = dp.GetObservations()
		assert.Error(t, err)

		assert.Equal(t, "strconv.ParseFloat: parsing \"foobar\": invalid syntax", err.Error())
	}
}

// Tests "GetObservations()" before Init() is called
func testGetObservationsNoInitFunc() func(*testing.T) {
	return func(t *testing.T) {
		dp := NewJsonProcessor()

		obs, err := dp.GetObservations()
		assert.NoError(t, err)
		assert.Nil(t, obs)
	}
}

// Tests "GetObservations()" called twice
func testGetObservationsTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"eventId": "eventId",
			"height":  "height",
			"rating":  "rating",
			"speed":   "speed",
			"target":  "target",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, measurements, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		assert.NoError(t, err)

		expectedFirstObservation := observations.Observation{
			Time: 1605312000,
			Measurements: map[string]float64{
				"eventId": 806.42,
				"height":  29,
				"rating":  86,
				"speed":   15,
				"target":  42,
			},
			Tags: []string{"testTagA", "testTagB"},
		}
		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

		actualObservations2, err := dp.GetObservations()
		assert.NoError(t, err)
		assert.Nil(t, actualObservations2)
	}
}

// Tests "GetObservations()" updated with same data
func testGetObservationsSameDataFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"eventId": "eventId",
			"height":  "height",
			"rating":  "rating",
			"speed":   "speed",
			"target":  "target",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, measurements, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		assert.NoError(t, err)

		expectedFirstObservation := observations.Observation{
			Time: 1605312000,
			Measurements: map[string]float64{
				"eventId": 806.42,
				"height":  29,
				"rating":  86,
				"speed":   15,
				"target":  42,
			},
			Tags: []string{"testTagA", "testTagB"},
		}
		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

		reader := bytes.NewReader(data)
		buffer := new(bytes.Buffer)
		_, err = io.Copy(buffer, reader)
		if err != nil {
			t.Error(err)
		}

		_, err = dp.OnData(buffer.Bytes())
		assert.NoError(t, err)

		actualObservations2, err := dp.GetObservations()
		assert.NoError(t, err)
		assert.Nil(t, actualObservations2)
	}
}
