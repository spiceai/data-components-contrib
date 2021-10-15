package flux

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/stretchr/testify/assert"
)

func TestFlux(t *testing.T) {
	data, err := os.ReadFile("../../test/assets/data/annotated-csv/cpu_metrics_influxdb_annotated.csv")
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

		dp := NewFluxCsvProcessor()
		err := dp.Init(map[string]string{
			"field": "_value",
		}, nil, nil)
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
			Measurements: map[string]float64{
				"usage_idle": 99.56272495215877,
			},
			Tags: []string{"cpu-total", "DESKTOP-2BSF9I6"},
		}
		assert.Equal(t, expectedFirstObservation.Time, actualObservations[0].Time, "First Observation not correct")
		assert.Equal(t, expectedFirstObservation.Measurements, actualObservations[0].Measurements, "First Observation not correct")
		assert.ElementsMatch(t, expectedFirstObservation.Tags, actualObservations[0].Tags, "First Observation not correct")

		expectedObservationsBytes, err := os.ReadFile("../../test/assets/data/json/observations_TestFlux-GetObservations()-expected_observations.json")
		if err != nil {
			t.Fatal(err)
		}

		expectedObservations := make([]observations.Observation, 0, 100)
		err = json.Unmarshal(expectedObservationsBytes, &expectedObservations)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, len(expectedObservations), len(actualObservations), "Expected observations count different from actual")

		for idx, expectedObservation := range expectedObservations {
			assert.Equal(t, expectedObservation.Time, actualObservations[idx].Time)
			assert.Equal(t, expectedObservation.Measurements, actualObservations[idx].Measurements)
			assert.ElementsMatch(t, expectedObservation.Tags, actualObservations[idx].Tags)
		}
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
		}, nil, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		assert.NoError(t, err)

		expectedFirstObservation := observations.Observation{
			Time: 1629159360,
			Measurements: map[string]float64{
				"usage_idle": 99.56272495215877,
			},
			Tags: []string{"cpu-total", "DESKTOP-2BSF9I6"},
		}
		assert.Equal(t, expectedFirstObservation.Time, actualObservations[0].Time, "First Observation not correct")
		assert.Equal(t, expectedFirstObservation.Measurements, actualObservations[0].Measurements, "First Observation not correct")
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
		}, nil, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		assert.NoError(t, err)

		expectedFirstObservation := observations.Observation{
			Time: 1629159360,
			Measurements: map[string]float64{
				"usage_idle": 99.56272495215877,
			},
			Tags: []string{"cpu-total", "DESKTOP-2BSF9I6"},
		}
		assert.Equal(t, expectedFirstObservation.Time, actualObservations[0].Time, "First Observation not correct")
		assert.Equal(t, expectedFirstObservation.Measurements, actualObservations[0].Measurements, "First Observation not correct")
		assert.ElementsMatch(t, expectedFirstObservation.Tags, actualObservations[0].Tags, "First Observation not correct")

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations2, err := dp.GetObservations()
		assert.NoError(t, err)
		assert.Nil(t, actualObservations2)
	}
}
