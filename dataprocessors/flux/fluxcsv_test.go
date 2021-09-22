package flux

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataprocessors/flux"))

func TestFlux(t *testing.T) {
	data, err := ioutil.ReadFile("../../test/assets/data/annotated-csv/cpu_metrics_influxdb_annotated.csv")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Init()", testInitFunc())
	t.Run("GetObservations() -o observations.json", testGetObservationsFunc(data))
	t.Run("GetObservations() called twice -o observations.json", testGetObservationsTwiceFunc(data))
	t.Run("GetObservations() same data -o observations.json", testGetObservationsSameDataFunc(data))
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewFluxCsvProcessor()

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

		dp := NewFluxCsvProcessor()
		err := dp.Init(map[string]string{
			"field": "_value",
		})
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		if err != nil {
			t.Error(err)
			return
		}

		expectedFirstObservation := observations.Observation{
			Time: 1629159360,
			Data: map[string]float64{
				"usage_idle": 99.56272495215877,
			},
			Tags: []string{"cpu-total", "DESKTOP-2BSF9I6"},
		}
		assert.Equal(t, expectedFirstObservation.Time, actualObservations[0].Time, "First Observation not correct")
		assert.Equal(t, expectedFirstObservation.Data, actualObservations[0].Data, "First Observation not correct")
		assert.ElementsMatch(t, expectedFirstObservation.Tags, actualObservations[0].Tags, "First Observation not correct")

		// marshal to JSON so the snapshot is easy to consume
		data, err := json.MarshalIndent(actualObservations, "", "  ")
		if err != nil {
			t.Fatal(err)
		}

		// TODO: Change this to load the test data from JSON and compare each element individually,
		// with an order insensitive compare for the tags
		snapshotter.SnapshotT(t, data)
	}
}

// Tests "GetObservations() called twice"
func testGetObservationsTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewFluxCsvProcessor()
		err := dp.Init(map[string]string{
			"field": "_value",
		})
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		assert.NoError(t, err)

		expectedFirstObservation := observations.Observation{
			Time: 1629159360,
			Data: map[string]float64{
				"usage_idle": 99.56272495215877,
			},
			Tags: []string{"cpu-total", "DESKTOP-2BSF9I6"},
		}
		assert.Equal(t, expectedFirstObservation.Time, actualObservations[0].Time, "First Observation not correct")
		assert.Equal(t, expectedFirstObservation.Data, actualObservations[0].Data, "First Observation not correct")
		assert.ElementsMatch(t, expectedFirstObservation.Tags, actualObservations[0].Tags, "First Observation not correct")

		actualObservations2, err := dp.GetObservations()
		assert.NoError(t, err)
		assert.Nil(t, actualObservations2)
	}
}

// Tests "GetObservations() updated with same data"
func testGetObservationsSameDataFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewFluxCsvProcessor()
		err := dp.Init(map[string]string{
			"field": "_value",
		})
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		assert.NoError(t, err)

		expectedFirstObservation := observations.Observation{
			Time: 1629159360,
			Data: map[string]float64{
				"usage_idle": 99.56272495215877,
			},
			Tags: []string{"cpu-total", "DESKTOP-2BSF9I6"},
		}
		assert.Equal(t, expectedFirstObservation.Time, actualObservations[0].Time, "First Observation not correct")
		assert.Equal(t, expectedFirstObservation.Data, actualObservations[0].Data, "First Observation not correct")
		assert.ElementsMatch(t, expectedFirstObservation.Tags, actualObservations[0].Tags, "First Observation not correct")

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations2, err := dp.GetObservations()
		assert.NoError(t, err)
		assert.Nil(t, actualObservations2)
	}
}
