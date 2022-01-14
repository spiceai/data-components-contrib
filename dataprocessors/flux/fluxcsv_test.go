package flux

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataprocessors/fluxcsv"))

func TestFlux(t *testing.T) {
	data, err := os.ReadFile("../../test/assets/data/annotated-csv/cpu_metrics_influxdb_annotated.csv")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Init()", testInitFunc())
	t.Run("GetRecord() -o observations.json", testGetRecordFunc(data))
	t.Run("GetRecord() called twice -o observations.json", testGetRecordTwiceFunc(data))
	t.Run("GetRecord() same data -o observations.json", testGetRecordSameDataFunc(data))
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewFluxCsvProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params, nil, nil, nil, nil)
		assert.NoError(t, err)
	}
}

// Tests "GetRecord()"
func testGetRecordFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewFluxCsvProcessor()
		err := dp.Init(map[string]string{
			"field": "_value",
		}, nil, nil, nil, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		if err != nil {
			t.Error(err)
			return
		}

		fields := []arrow.Field{
			{Name: "_time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.usage_idle", Type: arrow.PrimitiveTypes.Float64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1629159360}, nil)
		recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{99.56272495215877}, nil)
		listBuilder := recordBuilder.Field(2).(*array.ListBuilder)
		valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
		listBuilder.Append(true)
		valueBuilder.Append("cpu-total")
		valueBuilder.Append("DESKTOP-2BSF9I6")

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")
		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetRecord() called twice"
func testGetRecordTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewFluxCsvProcessor()
		err := dp.Init(map[string]string{"field": "_value"}, nil, nil, nil, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		assert.NoError(t, err)

		fields := []arrow.Field{
			{Name: "_time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.usage_idle", Type: arrow.PrimitiveTypes.Float64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1629159360}, nil)
		recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{99.56272495215877}, nil)
		listBuilder := recordBuilder.Field(2).(*array.ListBuilder)
		valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
		listBuilder.Append(true)
		valueBuilder.Append("cpu-total")
		valueBuilder.Append("DESKTOP-2BSF9I6")

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualRecord.NewSlice(0, 1)), "First Record not correct")

		actualRecord2, err := dp.GetRecord()
		assert.NoError(t, err)
		assert.Nil(t, actualRecord2)
	}
}

// Tests "GetRecord() updated with same data"
func testGetRecordSameDataFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewFluxCsvProcessor()
		err := dp.Init(map[string]string{
			"field": "_value",
		}, nil, nil, nil, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		assert.NoError(t, err)

		fields := []arrow.Field{
			{Name: "_time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.usage_idle", Type: arrow.PrimitiveTypes.Float64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1629159360}, nil)
		recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{99.56272495215877}, nil)
		listBuilder := recordBuilder.Field(2).(*array.ListBuilder)
		valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
		listBuilder.Append(true)
		valueBuilder.Append("cpu-total")
		valueBuilder.Append("DESKTOP-2BSF9I6")

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
