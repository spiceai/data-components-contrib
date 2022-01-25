package csv

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataprocessors/csv"))

func TestCsv(t *testing.T) {
	localData, err := os.ReadFile("../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
	if err != nil {
		t.Fatal(err.Error())
	}
	localDataTags, err := os.ReadFile("../../test/assets/data/csv/local_tag_data.csv")
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Run("Init()", testInitFunc())
	t.Run("GetRecord()", testGetRecordFunc(localData, false))
	t.Run("GetRecord() dirty data", testGetRecordDirtyDataFunc())
	t.Run("GetRecord() identifiers", testGetRecordWithIdentifiersFunc())
	t.Run("GetRecord() custom time format", testGetRecordCustomTimeFunc())
	t.Run("GetRecord() with tags", testGetRecordFunc(localDataTags, true))
	t.Run("GetRecord() called twice", testGetRecordTwiceFunc(localData))
	t.Run("GetRecord() updated with same data", testGetRecordSameDataFunc(localData))
	t.Run("GetRecord() with partial column selection", testGetRecordPartialColumns(localData))

}

func BenchmarkGetRecord(b *testing.B) {
	data, err := os.ReadFile("../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
	if err != nil {
		b.Error(err)
	}
	b.Run("GetRecord()", benchGetRecordFunc(data))
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewCsvProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params, nil, nil, nil, nil)
		assert.NoError(t, err)
	}
}

// Tests "GetRecord()"
func testGetRecordFunc(data []byte, withTags bool) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"open":  "open",
			"high":  "high",
			"low":   "low",
			"close": "close",
			"vol":   "volume",
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

		actualRecord, err := dp.GetRecord()
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
			{Name: "measure.vol", Type: arrow.PrimitiveTypes.Float64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
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
		listBuilder := recordBuilder.Field(6).(*array.ListBuilder)
		defer listBuilder.Release()
		valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
		listBuilder.Append(true)
		if withTags {
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

// Tests "GetRecord()" - dirty data
func testGetRecordDirtyDataFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := os.ReadFile("../../test/assets/data/csv/dirty_data.csv")
		if err != nil {
			t.Fatal(err.Error())
		}

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
		err = dp.Init(nil, nil, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
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
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
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
		listBuilder := recordBuilder.Field(6).(*array.ListBuilder)
		defer listBuilder.Release()
		valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
		listBuilder.Append(true)
		valueBuilder.Append("elon_tweet")
		valueBuilder.Append("market_open")
		valueBuilder.Append("tagA")

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")
		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetRecord() - custom time format"
func testGetRecordCustomTimeFunc() func(*testing.T) {
	return func(t *testing.T) {
		epoch := time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)
		period := 7 * 24 * time.Hour
		interval := 24 * time.Hour

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
			"path":  "../../test/assets/data/csv/custom_time.csv",
			"watch": "false",
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		if len(localData) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"val": "val",
		}

		categories := map[string]string{}

		dp := NewCsvProcessor()
		err = dp.Init(map[string]string{
			"time_format": "2006-01-02 15:04:05-07:00",
		}, nil, measurements, categories, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(localData)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		if err != nil {
			t.Error(err)
			return
		}

		fields := []arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.val", Type: arrow.PrimitiveTypes.Float64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1547575074}, nil)
		recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{34}, nil)
		listBuilder := recordBuilder.Field(2).(*array.ListBuilder)
		defer listBuilder.Release()
		listBuilder.Append(true)

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")
		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetRecord()" identifiers
func testGetRecordWithIdentifiersFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := os.ReadFile("../../test/assets/data/csv/btcusd_ticks.csv")
		if err != nil {
			t.Fatal(err.Error())
		}

		if len(data) == 0 {
			t.Fatal("no data")
		}

		identifiers := map[string]string{
			"tick_id": "tick_id",
		}

		measurements := map[string]string{
			"open":   "open",
			"high":   "high",
			"low":    "low",
			"close":  "close",
			"volume": "volume",
		}

		dp := NewCsvProcessor()
		err = dp.Init(nil, identifiers, measurements, nil, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
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
			{Name: "id.tick_id", Type: arrow.BinaryTypes.String},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
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
		recordBuilder.Field(6).(*array.StringBuilder).AppendValues([]string{"1"}, nil)
		listBuilder := recordBuilder.Field(7).(*array.ListBuilder)
		defer listBuilder.Release()
		listBuilder.Append(true)

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")
		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetRecord()" called twice
func testGetRecordTwiceFunc(data []byte) func(*testing.T) {
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

		dp := NewCsvProcessor()
		err := dp.Init(nil, nil, measurements, categories, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		assert.NoError(t, err)

		fields := []arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.open", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.high", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.low", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.close", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.volume", Type: arrow.PrimitiveTypes.Float64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
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
		listBuilder := recordBuilder.Field(6).(*array.ListBuilder)
		defer listBuilder.Release()
		listBuilder.Append(true)

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")

		actualRecord2, err := dp.GetRecord()
		assert.NoError(t, err)
		assert.Nil(t, actualRecord2)
	}
}

// Tests "GetRecord()" updated with same data
func testGetRecordSameDataFunc(data []byte) func(*testing.T) {
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

		dp := NewCsvProcessor()
		err := dp.Init(nil, nil, measurements, categories, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		assert.NoError(t, err)

		fields := []arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.open", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.high", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.low", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.close", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.volume", Type: arrow.PrimitiveTypes.Float64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
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
		listBuilder := recordBuilder.Field(6).(*array.ListBuilder)
		defer listBuilder.Release()
		listBuilder.Append(true)

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord2, err := dp.GetRecord()
		assert.NoError(t, err)
		assert.Nil(t, actualRecord2)
	}
}

// Tests "GetRecord()" with partial columns selection
func testGetRecordPartialColumns(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"open":   "open",
			"close":  "close",
			"volume": "volume",
		}

		categories := map[string]string{}

		dp := NewCsvProcessor()
		err := dp.Init(nil, nil, measurements, categories, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		assert.NoError(t, err)

		fields := []arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.open", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.close", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.volume", Type: arrow.PrimitiveTypes.Float64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1605312000}, nil)
		recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{16339.56}, nil)
		recordBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{16254.51}, nil)
		recordBuilder.Field(3).(*array.Float64Builder).AppendValues([]float64{274.42607}, nil)
		listBuilder := recordBuilder.Field(4).(*array.ListBuilder)
		defer listBuilder.Release()
		listBuilder.Append(true)

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")
	}
}

// Benchmark "GetRecord()"
func benchGetRecordFunc(data []byte) func(*testing.B) {
	return func(b *testing.B) {
		measurements := map[string]string{
			"open":   "open",
			"high":   "high",
			"low":    "low",
			"close":  "close",
			"volume": "volume",
		}
		categories := map[string]string{}

		dp := NewCsvProcessor()
		err := dp.Init(nil, nil, measurements, categories, nil)
		if err != nil {
			b.Error(err)
		}

		for i := 0; i < 10; i++ {
			dp.OnData(data)
			_, err := dp.GetRecord()
			if err != nil {
				b.Fatal(err.Error())
			}
		}
	}
}
