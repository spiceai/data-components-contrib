package flux

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/influxdata/flux"
	flux_array "github.com/influxdata/flux/array"
	flux_csv "github.com/influxdata/flux/csv"
	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/util"
	"go.uber.org/zap"
)

var (
	zaplog *zap.Logger = loggers.ZapLogger()
)

const (
	FluxCsvProcessorName string = "flux-csv"
)

type FluxCsvProcessor struct {
	data      []byte
	dataMutex sync.RWMutex
	dataHash  []byte
}

func NewFluxCsvProcessor() *FluxCsvProcessor {
	return &FluxCsvProcessor{}
}

func (p *FluxCsvProcessor) Init(params map[string]string, identifiers map[string]string, measurements map[string]string, categories map[string]string, tags []string) error {
	return nil
}

func (p *FluxCsvProcessor) OnData(data []byte) ([]byte, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	newDataHash, err := util.ComputeNewHash(p.data, p.dataHash, data)
	if err != nil {
		return nil, fmt.Errorf("error computing new data hash in %s processor: %w", FluxCsvProcessorName, err)
	}

	if newDataHash != nil {
		// Only update data if new
		p.data = data
		p.dataHash = newDataHash
	}

	return data, nil
}

func (p *FluxCsvProcessor) GetRecord() (array.Record, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	if len(p.data) == 0 {
		return nil, nil
	}

	reader := bytes.NewReader(p.data)
	readCloser := io.NopCloser(reader)

	decoder := flux_csv.NewMultiResultDecoder(flux_csv.ResultDecoderConfig{ /* Use defaults */ })
	results, err := decoder.Decode(readCloser)
	if err != nil {
		return nil, err
	}
	defer results.Release()

	arrow_fields := []arrow.Field{}

	pool := memory.NewGoAllocator()

	timeBuilder := array.NewInt64Builder(pool)
	defer timeBuilder.Release()

	valueBuilder := array.NewFloat64Builder(pool)
	defer valueBuilder.Release()

	tagListBuilder := array.NewListBuilder(pool, arrow.BinaryTypes.String)
	defer tagListBuilder.Release()
	tagValueBuilder := tagListBuilder.ValueBuilder().(*array.StringBuilder)
	defer tagValueBuilder.Release()

	for results.More() {
		result := results.Next()

		err = result.Tables().Do(func(t flux.Table) error {
			return t.Do(func(colReader flux.ColReader) error {
				defer colReader.Release()

				timeIndex := -1
				fieldIndex := -1
				valueIndex := -1
				var tagIndices []int
				for colIndex, colMeta := range colReader.Cols() {
					// We currently only support one field and float for now
					switch {
					case colMeta.Label == "_time":
						timeIndex = colIndex
					case colMeta.Label == "_field":
						fieldIndex = colIndex
					case colMeta.Label == "_value":
						valueIndex = colIndex
					case colMeta.Label == "_measurement" || colMeta.Type.String() != "string":
						continue
					default:
						tagIndices = append(tagIndices, colIndex)
					}
				}

				if timeIndex == -1 {
					return errors.New("'_time' not found in table data")
				}

				if fieldIndex == -1 {
					return errors.New("'_field' not found in table data")
				}

				if valueIndex == -1 {
					return errors.New("'_value' not found in table data")
				}

				times := colReader.Times(timeIndex)
				defer times.Release()

				fields := colReader.Strings(fieldIndex)
				defer fields.Release()

				values := colReader.Floats(valueIndex)
				defer values.Release()

				var tags []*flux_array.String
				for _, tagIndex := range tagIndices {
					tags = append(tags, colReader.Strings(tagIndex))
				}

				if len(arrow_fields) == 0 {
					arrow_fields = []arrow.Field{
						{Name: "time", Type: arrow.PrimitiveTypes.Int64},
						{Name: fmt.Sprintf("measure.%s", fields.Value(0)), Type: arrow.PrimitiveTypes.Float64},
						{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
					}
				}

				secondScale := int64(time.Second)
				for i := 0; i < colReader.Len(); i++ {
					tagListBuilder.Append(true)
					if times.IsValid(i) && !times.IsNull(i) &&
						fields.IsValid(i) && !fields.IsNull(i) &&
						values.IsValid(i) && !values.IsNull(i) {

						for _, tagValue := range tags {
							if tagValue.IsValid(i) && !tagValue.IsNull(i) {
								tagValueBuilder.Append(tagValue.Value(i))
							}
						}

						timeBuilder.Append(times.Value(i) / secondScale)
						valueBuilder.Append(values.Value(i))
					}
				}

				for _, tagCol := range tags {
					tagCol.Release()
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}

	err = results.Err()
	if err != nil {
		zaplog.Sugar().Warnf("error decoding flux csv result: %w", err)
		return nil, err
	}

	p.data = nil

	record := array.NewRecord(
		arrow.NewSchema(arrow_fields, nil),
		[]array.Interface{timeBuilder.NewArray(), valueBuilder.NewArray(), tagListBuilder.NewArray()},
		int64(timeBuilder.Len()))

	return record, nil
}
