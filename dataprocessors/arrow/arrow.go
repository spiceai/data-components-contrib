package arrow

import (
	"fmt"
	"unsafe"

	apache_arrow "github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/flight"
	"github.com/apache/arrow/go/v7/arrow/memory"
)

const (
	ArrowProcessorName string = "arrow"
)

type ArrowProcessor struct {
	timeSelector string
	identifiers  map[string]string
	measurements map[string]string
	categories   map[string]string
	tags         []string

	streamPointer *flight.FlightService_DoGetClient
}

func NewArrowProcessor() *ArrowProcessor {
	return &ArrowProcessor{}
}

func (p *ArrowProcessor) Init(params map[string]string, identifiers map[string]string, measurements map[string]string, categories map[string]string, tags []string) error {
	if selector, ok := params["time_selector"]; ok && selector != "" {
		p.timeSelector = selector
	} else {
		p.timeSelector = "time"
	}

	p.identifiers = identifiers
	p.measurements = measurements
	p.categories = categories
	p.tags = tags

	return nil
}

func (p *ArrowProcessor) OnData(data []byte) ([]byte, error) {
	p.streamPointer = (*flight.FlightService_DoGetClient)(unsafe.Pointer(&data))
	return data, nil
}

type FieldInfo struct {
	Index int
	Field apache_arrow.Field
}

func (p *ArrowProcessor) GetRecord() (apache_arrow.Record, error) {
	if p.streamPointer == nil {
		return nil, nil
	}

	reader, err := flight.NewRecordReader(*p.streamPointer)
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	if reader.Next() {
		record := reader.Record()

		// Creating field map for quick look-up from field name
		fieldMap := make(map[string]FieldInfo)
		for fieldIndex, field := range record.Schema().Fields() {
			fieldMap[field.Name] = FieldInfo{Index: fieldIndex, Field: field}
		}

		// Checking time column is present
		timeField, ok := fieldMap[p.timeSelector]
		if !ok {
			return nil, fmt.Errorf("time column '%s' not found", p.timeSelector)
		}
		if timeField.Field.Type != apache_arrow.PrimitiveTypes.Int64 {
			return nil, fmt.Errorf("time column '%s' type mistmach", p.timeSelector)
		}

		// Creating new record: new schema + new columns
		pool := memory.NewGoAllocator()
		newFields := []apache_arrow.Field{timeField.Field}
		newColumns := []apache_arrow.Array{record.Columns()[timeField.Index]}

		for outputName, inputName := range p.identifiers {
			fieldInfo, ok := fieldMap[inputName]
			if !ok {
				return nil, fmt.Errorf("identifier column '%s' not found", inputName)
			}
			if fieldInfo.Field.Type != apache_arrow.BinaryTypes.String {
				return nil, fmt.Errorf("identifier column '%s' type mistmach", inputName)
			}
			newFields = append(newFields, apache_arrow.Field{Name: fmt.Sprintf("id.%s", outputName), Type: fieldInfo.Field.Type})
			newColumns = append(newColumns, record.Columns()[fieldInfo.Index])
		}
		for outputName, inputName := range p.measurements {
			fieldInfo, ok := fieldMap[inputName]
			if !ok {
				return nil, fmt.Errorf("measurement column '%s' not found", inputName)
			}
			// Converting type if needed
			if fieldInfo.Field.Type == apache_arrow.PrimitiveTypes.Float64 {
				newColumns = append(newColumns, record.Columns()[fieldInfo.Index])
			} else if fieldInfo.Field.Type == apache_arrow.PrimitiveTypes.Int64 {
				arrayBuilder := array.NewFloat64Builder(pool)
				defer arrayBuilder.Release()
				column := record.Columns()[fieldInfo.Index].(*array.Int64)
				for entryIndex := 0; entryIndex < int(record.NumRows()); entryIndex++ {
					if column.IsNull(entryIndex) {
						arrayBuilder.AppendNull()
					} else {
						arrayBuilder.Append(float64(column.Value(entryIndex)))
					}
				}
				newColumns = append(newColumns, arrayBuilder.NewArray())
			} else {
				return nil, fmt.Errorf("measurement column '%s' type mistmach", inputName)
			}
			newFields = append(newFields, apache_arrow.Field{
				Name: fmt.Sprintf("measure.%s", outputName), Type: apache_arrow.PrimitiveTypes.Float64})
		}
		for outputName, inputName := range p.categories {
			fieldInfo, ok := fieldMap[inputName]
			if !ok {
				return nil, fmt.Errorf("category column '%s' not found", inputName)
			}
			if fieldInfo.Field.Type != apache_arrow.BinaryTypes.String {
				return nil, fmt.Errorf("category column '%s' type mistmach", inputName)
			}
			newFields = append(newFields, apache_arrow.Field{Name: fmt.Sprintf("cat.%s", outputName), Type: fieldInfo.Field.Type})
			newColumns = append(newColumns, record.Columns()[fieldInfo.Index])
		}
		for _, inputName := range p.tags {
			fieldInfo, ok := fieldMap[inputName]
			if !ok {
				return nil, fmt.Errorf("tag column '%s' not found", inputName)
			}
			if fieldInfo.Field.Type != apache_arrow.BinaryTypes.String {
				return nil, fmt.Errorf("tag column '%s' type mistmach", inputName)
			}
			newFields = append(newFields, apache_arrow.Field{Name: fmt.Sprintf("tag.%s", inputName), Type: fieldInfo.Field.Type})
			newColumns = append(newColumns, record.Columns()[fieldInfo.Index])
		}

		newRecord := array.NewRecord(apache_arrow.NewSchema(newFields, nil), newColumns, record.NumRows())
		return newRecord, nil
	}

	return nil, fmt.Errorf("no record could be read")
}
