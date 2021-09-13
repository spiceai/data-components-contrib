package json

import (
	"bytes"
	"io"
	"os"
	"sort"
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

	invalid_data, err := os.ReadFile("../../test/assets/data/json/invalid_schema.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	invalid_time, err := os.ReadFile("../../test/assets/data/json/mock_array_invalid_time.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Run("Init()", testInitFunc())
	t.Run("GetObservations()", testGetObservationsFunc(data))
	t.Run("GetObservations() called twice", testGetObservationsTwiceFunc(data))
	t.Run("GetObservations() updated with same data", testGetObservationsSameDataFunc(data))
	t.Run("OnData() called with invalid schema", testOnDataInvalidSchema(invalid_data, "0: (root): Invalid type. Expected: array, given: object"))
	t.Run("OnData() called with invalid time", testOnDataInvalidSchema(invalid_time, "0: 0.time: Must validate at least one schema (anyOf)"))
	//t.Run("GetState()", testGetStateFunc(data))
	//t.Run("GetState() called twice", testGetStateTwiceFunc(data))
	//t.Run("getColumnMappings()", testgetColumnMappingsFunc())
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewJsonProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params)
		assert.NoError(t, err)
	}
}

// Tests "GetObservations()"
func testGetObservationsFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil)
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
			Data: map[string]float64{
				"eventId": 806.42,
				"height":  29,
				"rating":  86,
				"speed":   15,
				"target":  42,
			},
		}
		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

		snapshotter.SnapshotT(t, actualObservations)
	}
}

// Tests "GetObservations()" called twice
func testGetObservationsTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		assert.NoError(t, err)

		expectedFirstObservation := observations.Observation{
			Time: 1605312000,
			Data: map[string]float64{
				"eventId": 806.42,
				"height":  29,
				"rating":  86,
				"speed":   15,
				"target":  42,
			},
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

		dp := NewJsonProcessor()
		err := dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		assert.NoError(t, err)

		expectedFirstObservation := observations.Observation{
			Time: 1605312000,
			Data: map[string]float64{
				"eventId": 806.42,
				"height":  29,
				"rating":  86,
				"speed":   15,
				"target":  42,
			},
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

func testOnDataInvalidSchema(data []byte, validationError string) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		if assert.Error(t, err) {
			assert.Equal(t, err.Error(), "schema violation")
			assert.IsType(t, &ValidationError{}, err)
			assert.Equal(t, validationError, err.(*ValidationError).validationError)
		}
	}
}

// Tests "GetState()"
func testGetStateFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualState, err := dp.GetState(nil)
		if err != nil {
			t.Error(err)
			return
		}

		assert.Equal(t, 2, len(actualState), "expected two state objects")

		sort.Slice(actualState, func(i, j int) bool {
			return actualState[i].Path() < actualState[j].Path()
		})

		assert.Equal(t, "coinbase.btcusd", actualState[0].Path(), "expected path incorrect")
		assert.Equal(t, "local.portfolio", actualState[1].Path(), "expected path incorrect")

		expectedFirstObservation := observations.Observation{
			Time: 1626697480,
			Data: map[string]float64{
				"price": 31232.709090909084,
			},
		}

		actualObservations := actualState[0].Observations()
		assert.Equal(t, expectedFirstObservation, actualState[0].Observations()[0], "First Observation not correct")
		assert.Equal(t, 57, len(actualObservations), "number of observations incorrect")

		expectedObservations := make([]observations.Observation, 0)
		assert.Equal(t, expectedObservations, actualState[1].Observations(), "Observations not correct")
	}
}

// Tests "GetState()" called twice
func testGetStateTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualState, err := dp.GetState(nil)
		if err != nil {
			t.Error(err)
			return
		}

		assert.Equal(t, 2, len(actualState), "expected two state objects")

		sort.Slice(actualState, func(i, j int) bool {
			return actualState[i].Path() < actualState[j].Path()
		})

		assert.Equal(t, "coinbase.btcusd", actualState[0].Path(), "expected path incorrect")
		assert.Equal(t, "local.portfolio", actualState[1].Path(), "expected path incorrect")

		expectedFirstObservation := observations.Observation{
			Time: 1626697480,
			Data: map[string]float64{
				"price": 31232.709090909084,
			},
		}

		actualObservations := actualState[0].Observations()
		assert.Equal(t, expectedFirstObservation, actualState[0].Observations()[0], "First Observation not correct")
		assert.Equal(t, 57, len(actualObservations), "number of observations incorrect")

		expectedObservations := make([]observations.Observation, 0)
		assert.Equal(t, expectedObservations, actualState[1].Observations(), "Observations not correct")

		actualState2, err := dp.GetState(nil)
		assert.NoError(t, err)
		assert.Nil(t, actualState2)
	}
}

// Tests "getColumnMappings()"
func testgetColumnMappingsFunc() func(*testing.T) {
	return func(t *testing.T) {
		headers := []string{"time", "local.portfolio.usd_balance", "local.portfolio.btc_balance", "coinbase.btcusd.price"}

		colToPath, colToFieldName, err := getColumnMappings(headers)
		if err != nil {
			t.Error(err)
			return
		}

		expectedColToPath := []string{"local.portfolio", "local.portfolio", "coinbase.btcusd"}
		assert.Equal(t, expectedColToPath, colToPath, "column to path mapping incorrect")

		expectedColToFieldName := []string{"usd_balance", "btc_balance", "price"}
		assert.Equal(t, expectedColToFieldName, colToFieldName, "column to path mapping incorrect")
	}
}
