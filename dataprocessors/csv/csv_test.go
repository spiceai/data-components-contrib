package csv

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataprocessors/csv"))

func TestCsv(t *testing.T) {
	epoch := time.Unix(1605312000, 0)
	period := 7 * 24 * time.Hour
	interval := time.Hour

	var wg sync.WaitGroup

	localFileConnector := file.NewFileConnector()

	var localData []byte
	err := localFileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		localData = data
		wg.Done()
		return nil, nil
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	wg.Add(1)

	err = localFileConnector.Init(epoch, period, interval, map[string]string{
		"path":  "../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv",
		"watch": "false",
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	localDataTags, err := os.ReadFile("../../test/assets/data/csv/local_tag_data.csv")
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Run("Init()", testInitFunc())
	t.Run("GetObservations()", testGetObservationsFunc(localData))
	// t.Run("GetObservations() dirty data", testGetObservationsDirtyDataFunc())
	// t.Run("GetObservations() identifiers", testGetObservationsWithIdentifiersFunc())
	// t.Run("GetObservations() custom time format", testGetObservationsCustomTimeFunc())
	t.Run("GetObservations() with tags", testGetObservationsFunc(localDataTags))
	// t.Run("GetObservations() called twice", testGetObservationsTwiceFunc(localData))
	// t.Run("GetObservations() updated with same data", testGetObservationsSameDataFunc(localData))

}

// func BenchmarkGetObservations(b *testing.B) {
// 	epoch := time.Unix(1605312000, 0)
// 	period := 7 * 24 * time.Hour
// 	interval := time.Hour

// 	localFileConnector := file.NewFileConnector()

// 	err := localFileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
// 		return nil, nil
// 	})
// 	if err != nil {
// 		b.Fatal(err.Error())
// 	}

// 	err = localFileConnector.Init(epoch, period, interval, map[string]string{
// 		"path":  "../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv",
// 		"watch": "false",
// 	})
// 	if err != nil {
// 		b.Error(err)
// 	}

// 	b.Run("GetObservations()", benchGetObservationsFunc(localFileConnector))
// }

// func TestGetFieldMappingsCsv(t *testing.T) {
// 	headers := map[string]int{
// 		"open":   10,
// 		"high":   9,
// 		"low":    8,
// 		"extra":  7,
// 		"close":  6,
// 		"volume": 5,
// 		"_tags":  4,
// 	}

// 	measurements := map[string]string{
// 		"open":   "open",
// 		"high":   "high",
// 		"low":    "low",
// 		"close":  "close",
// 		"volume": "volume",
// 	}

// 	mappings := getFieldMappings(measurements, headers)

// 	expectedMappings := map[string]int{
// 		"open":   headers["open"],
// 		"high":   headers["high"],
// 		"low":    headers["low"],
// 		"close":  headers["close"],
// 		"volume": headers["volume"],
// 	}

// 	assert.Equal(t, expectedMappings, mappings)
// }

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewCsvProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params, nil, nil, nil, nil)
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
			"open":   "open",
			"high":   "high",
			"low":    "low",
			"close":  "close",
			"volume": "volume",
		}

		categories := map[string]string{}

		tags := []string{
			"_tags",
			"tag1",
		}

		dp := NewCsvProcessor()
		err := dp.Init(nil, nil, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetObservations()
		if err != nil {
			t.Error(err)
			return
		}

		fields := []arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.open", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.high", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.low", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.close", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.volume", Type: arrow.PrimitiveTypes.Float64},
		}
		with_tags := false
		for _, field := range actualRecord.Schema().Fields() {
			if field.Name == "tags" {
				fields = append(fields, arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)})
				with_tags = true
				break
			}
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1605312000}, nil)
		recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{16339.56}, nil)
		recordBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{16339.6}, nil)
		recordBuilder.Field(3).(*array.Float64Builder).AppendValues([]float64{16240}, nil)
		recordBuilder.Field(4).(*array.Float64Builder).AppendValues([]float64{16254.51}, nil)
		recordBuilder.Field(5).(*array.Float64Builder).AppendValues([]float64{274.42607}, nil)
		if with_tags {
			listBuilder := recordBuilder.Field(6).(*array.ListBuilder)
			valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
			listBuilder.Append(true)
			valueBuilder.Append("elon_tweet")
			valueBuilder.Append("market_open")
			valueBuilder.Append("tagA")
		}

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")
		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetObservations()" - dirty data
// func testGetObservationsDirtyDataFunc() func(*testing.T) {
// 	return func(t *testing.T) {
// 		data, err := os.ReadFile("../../test/assets/data/csv/dirty_data.csv")
// 		if err != nil {
// 			t.Fatal(err.Error())
// 		}

// 		if len(data) == 0 {
// 			t.Fatal("no data")
// 		}

// 		measurements := map[string]string{
// 			"open":   "open",
// 			"high":   "high",
// 			"low":    "low",
// 			"close":  "close",
// 			"volume": "volume",
// 		}

// 		categories := map[string]string{}

// 		tags := []string{
// 			"_tags",
// 			"tag1",
// 		}

// 		dp := NewCsvProcessor()
// 		err = dp.Init(nil, nil, measurements, categories, tags)
// 		assert.NoError(t, err)

// 		_, err = dp.OnData(data)
// 		assert.NoError(t, err)

// 		actualObservations, err := dp.GetObservations()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		expectedFirstObservation := observations.Observation{
// 			Time: 1605312000,
// 			Measurements: map[string]float64{
// 				"open":   16339.56,
// 				"high":   16339.6,
// 				"low":    16240,
// 				"close":  16254.51,
// 				"volume": 274.42607,
// 			},
// 		}

// 		if len(actualObservations[0].Tags) > 0 {
// 			expectedFirstObservation.Tags = []string{
// 				"elon_tweet",
// 				"market_open",
// 				"tagA",
// 			}
// 		}

// 		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

// 		observationsJson, err := json.MarshalIndent(actualObservations, "", "  ")
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		snapshotter.SnapshotT(t, observationsJson)
// 	}
// }

// // Tests "GetObservations() - custom time format"
// func testGetObservationsCustomTimeFunc() func(*testing.T) {
// 	return func(t *testing.T) {
// 		epoch := time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)
// 		period := 7 * 24 * time.Hour
// 		interval := 24 * time.Hour

// 		var wg sync.WaitGroup

// 		localFileConnector := file.NewFileConnector()

// 		var localData []byte
// 		err := localFileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
// 			localData = data
// 			wg.Done()
// 			return nil, nil
// 		})
// 		if err != nil {
// 			t.Fatal(err.Error())
// 		}
// 		wg.Add(1)

// 		err = localFileConnector.Init(epoch, period, interval, map[string]string{
// 			"path":  "../../test/assets/data/csv/custom_time.csv",
// 			"watch": "false",
// 		})
// 		if err != nil {
// 			t.Fatal(err.Error())
// 		}

// 		if len(localData) == 0 {
// 			t.Fatal("no data")
// 		}

// 		measurements := map[string]string{
// 			"val": "val",
// 		}

// 		categories := map[string]string{}

// 		dp := NewCsvProcessor()
// 		err = dp.Init(map[string]string{
// 			"time_format": "2006-01-02 15:04:05-07:00",
// 		}, nil, measurements, categories, nil)
// 		assert.NoError(t, err)

// 		_, err = dp.OnData(localData)
// 		assert.NoError(t, err)

// 		actualObservations, err := dp.GetObservations()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		expectedFirstObservation := observations.Observation{
// 			Time: 1547575074,
// 			Measurements: map[string]float64{
// 				"val": 34,
// 			},
// 		}
// 		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

// 		observationsJson, err := json.MarshalIndent(actualObservations, "", "  ")
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		snapshotter.SnapshotT(t, observationsJson)
// 	}
// }

// // Tests "GetObservations()" identifiers
// func testGetObservationsWithIdentifiersFunc() func(*testing.T) {
// 	return func(t *testing.T) {
// 		data, err := os.ReadFile("../../test/assets/data/csv/btcusd_ticks.csv")
// 		if err != nil {
// 			t.Fatal(err.Error())
// 		}

// 		if len(data) == 0 {
// 			t.Fatal("no data")
// 		}

// 		identifiers := map[string]string{
// 			"tick_id": "tick_id",
// 		}

// 		measurements := map[string]string{
// 			"open":   "open",
// 			"high":   "high",
// 			"low":    "low",
// 			"close":  "close",
// 			"volume": "volume",
// 		}

// 		dp := NewCsvProcessor()
// 		err = dp.Init(nil, identifiers, measurements, nil, nil)
// 		assert.NoError(t, err)

// 		_, err = dp.OnData(data)
// 		assert.NoError(t, err)

// 		actualObservations, err := dp.GetObservations()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		expectedFirstObservation := observations.Observation{
// 			Time: 1605312000,
// 			Identifiers: map[string]string{
// 				"tick_id": "1",
// 			},
// 			Measurements: map[string]float64{
// 				"open":   16339.56,
// 				"high":   16339.6,
// 				"low":    16240,
// 				"close":  16254.51,
// 				"volume": 274.42607,
// 			},
// 		}

// 		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

// 		observationsJson, err := json.MarshalIndent(actualObservations, "", "  ")
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		snapshotter.SnapshotT(t, observationsJson)
// 	}
// }

// // Tests "GetObservations()" called twice
// func testGetObservationsTwiceFunc(data []byte) func(*testing.T) {
// 	return func(t *testing.T) {
// 		if len(data) == 0 {
// 			t.Fatal("no data")
// 		}

// 		measurements := map[string]string{
// 			"open":   "open",
// 			"high":   "high",
// 			"low":    "low",
// 			"close":  "close",
// 			"volume": "volume",
// 		}

// 		categories := map[string]string{}

// 		dp := NewCsvProcessor()
// 		err := dp.Init(nil, nil, measurements, categories, nil)
// 		assert.NoError(t, err)

// 		_, err = dp.OnData(data)
// 		assert.NoError(t, err)

// 		actualObservations, err := dp.GetObservations()
// 		assert.NoError(t, err)

// 		expectedFirstObservation := observations.Observation{
// 			Time: 1605312000,
// 			Measurements: map[string]float64{
// 				"open":   16339.56,
// 				"high":   16339.6,
// 				"low":    16240,
// 				"close":  16254.51,
// 				"volume": 274.42607,
// 			},
// 		}
// 		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

// 		actualObservations2, err := dp.GetObservations()
// 		assert.NoError(t, err)
// 		assert.Nil(t, actualObservations2)
// 	}
// }

// // Tests "GetObservations()" updated with same data
// func testGetObservationsSameDataFunc(data []byte) func(*testing.T) {
// 	return func(t *testing.T) {
// 		if len(data) == 0 {
// 			t.Fatal("no data")
// 		}

// 		measurements := map[string]string{
// 			"open":   "open",
// 			"high":   "high",
// 			"low":    "low",
// 			"close":  "close",
// 			"volume": "volume",
// 		}

// 		categories := map[string]string{}

// 		dp := NewCsvProcessor()
// 		err := dp.Init(nil, nil, measurements, categories, nil)
// 		assert.NoError(t, err)

// 		_, err = dp.OnData(data)
// 		assert.NoError(t, err)

// 		actualObservations, err := dp.GetObservations()
// 		assert.NoError(t, err)

// 		expectedFirstObservation := observations.Observation{
// 			Time: 1605312000,
// 			Measurements: map[string]float64{
// 				"open":   16339.56,
// 				"high":   16339.6,
// 				"low":    16240,
// 				"close":  16254.51,
// 				"volume": 274.42607,
// 			},
// 		}
// 		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

// 		reader := bytes.NewReader(data)
// 		buffer := new(bytes.Buffer)
// 		_, err = io.Copy(buffer, reader)
// 		if err != nil {
// 			t.Error(err)
// 		}

// 		_, err = dp.OnData(buffer.Bytes())
// 		assert.NoError(t, err)

// 		actualObservations2, err := dp.GetObservations()
// 		assert.NoError(t, err)
// 		assert.Nil(t, actualObservations2)
// 	}
// }

// // Benchmark "GetObservations()"
// func benchGetObservationsFunc(c *file.FileConnector) func(*testing.B) {
// 	return func(b *testing.B) {
// 		dp := NewCsvProcessor()
// 		err := dp.Init(nil, nil, nil, nil, nil)
// 		if err != nil {
// 			b.Error(err)
// 		}

// 		for i := 0; i < 10; i++ {
// 			_, err := dp.GetObservations()
// 			if err != nil {
// 				b.Fatal(err.Error())
// 			}
// 		}
// 	}
// }
