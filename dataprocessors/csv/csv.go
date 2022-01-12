package csv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"strings"

	"sync"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	arrow_csv "github.com/apache/arrow/go/arrow/csv"
	"github.com/apache/arrow/go/arrow/memory"

	// "github.com/spiceai/data-components-contrib/dataprocessors/conv"
	// spice_time "github.com/spiceai/spiceai/pkg/time"
	"github.com/spiceai/spiceai/pkg/util"
)

const (
	CsvProcessorName string = "csv"
	tagsColumnName   string = "_tags"
)

type CsvProcessor struct {
	timeFormat   string
	timeSelector string

	identifiers  map[string]string
	measurements map[string]string
	categories   map[string]string
	tags         []string

	dataMutex sync.RWMutex
	data      []byte
	dataHash  []byte

	currentRecord array.Record
}

func NewCsvProcessor() *CsvProcessor {
	return &CsvProcessor{}
}

func (p *CsvProcessor) Init(params map[string]string, identifiers map[string]string, measurements map[string]string, categories map[string]string, tags []string) error {
	if format, ok := params["time_format"]; ok {
		p.timeFormat = format
	}
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

func (p *CsvProcessor) OnData(data []byte) ([]byte, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	newDataHash, err := util.ComputeNewHash(p.data, p.dataHash, data)
	if err != nil {
		return nil, fmt.Errorf("error computing new data hash in csv processor: %w", err)
	}

	if newDataHash != nil {
		// Only update data if new
		p.data = data
		p.dataHash = newDataHash
	}

	return data, nil
}

func (p *CsvProcessor) GetObservations() (array.Record, error) {
	if p.data == nil {
		return nil, nil
	}

	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	if p.data == nil {
		return nil, nil
	}

	if len(p.data) == 0 {
		return nil, nil
	}

	err := p.getObservations(p.data)
	if err != nil {
		return nil, err
	}

	p.data = nil
	return p.currentRecord, nil
}

func (p *CsvProcessor) getObservations(data []byte) error {
	numTags := len(p.tags)

	if len(p.identifiers)+len(p.measurements)+len(p.categories)+numTags == 0 {
		return nil
	}

	identifiersMap := make(map[string]bool)
	measurementsMap := make(map[string]bool)
	categoriesMap := make(map[string]bool)
	tagsMap := make(map[string]bool)
	for _, key := range p.identifiers {
		identifiersMap[key] = true
	}
	for _, key := range p.measurements {
		measurementsMap[key] = true
	}
	for _, key := range p.categories {
		categoriesMap[key] = true
	}
	for _, key := range p.tags {
		tagsMap[key] = true
	}

	headers, err := getCsvHeader(data)
	if err != nil {
		return fmt.Errorf("failed to get csv header: %s", err)
	}

	fields := make([]arrow.Field, len(headers))
	var tagColumns []int
	timeCol := -1
	for i, header := range headers {
		// Allocate time column index upon finding timeSelector if not yet assigned
		if timeCol < 0 && header == p.timeSelector {
			timeCol = i
			fields[i].Name = header
			fields[i].Type = arrow.PrimitiveTypes.Int64
		} else {
			switch {
			case identifiersMap[header]:
				fields[i].Name = fmt.Sprintf("id.%s", header)
				fields[i].Type = arrow.BinaryTypes.String
			case measurementsMap[header]:
				fields[i].Name = fmt.Sprintf("measure.%s", header)
				fields[i].Type = arrow.PrimitiveTypes.Float64
			case categoriesMap[header]:
				fields[i].Name = fmt.Sprintf("cat.%s", header)
				fields[i].Type = arrow.BinaryTypes.String
			case tagsMap[header]:
				tagColumns = append(tagColumns, i)
				fields[i].Name = fmt.Sprintf("tag.%s", header)
				fields[i].Type = arrow.BinaryTypes.String
			default:
				return fmt.Errorf("Unexpected error header \"%s\", not found in maps", header)
			}
		}
	}

	if timeCol < 0 {
		return fmt.Errorf("time header '%s' not found", p.timeSelector)
	}

	schema := arrow.NewSchema(fields, nil)
	arrowReader := arrow_csv.NewReader(
		bytes.NewBuffer(data), schema, arrow_csv.WithHeader(true), arrow_csv.WithChunk(-1),
		arrow_csv.WithNullReader(false))
	defer arrowReader.Release()

	arrowReader.Next()

	if p.currentRecord != nil {
		p.currentRecord.Release()
	}
	record := arrowReader.Record()
	// Re allocating the name that were overwriten by the CSV reader
	for i, field := range fields {
		record.Schema().Fields()[i].Name = field.Name
	}

	if len(tagColumns) > 0 {
		// Aggregating tags
		pool := memory.NewGoAllocator()
		tagListBuilder := array.NewListBuilder(pool, arrow.BinaryTypes.String)
		tagValueBuilder := tagListBuilder.ValueBuilder().(*array.StringBuilder)
		defer tagListBuilder.Release()

		for i := 0; i < int(record.NumRows()); i++ {
			tagListBuilder.Append(true)
			for _, colIndex := range tagColumns {
				tagCol := record.Columns()[colIndex].(*array.String)
				for _, tag := range strings.Fields(tagCol.Value(i)) {
					tagValueBuilder.Append(tag)
				}
			}
		}

		// Creating a new record without old tag columns but aggregated one
		var new_fields []arrow.Field
		var new_columns []array.Interface
		for i, field := range fields {
			if field.Name[:4] != "tag." {
				new_fields = append(new_fields, field)
				new_columns = append(new_columns, record.Column(i))
			}
		}
		new_fields = append(new_fields, arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)})
		new_columns = append(new_columns, tagListBuilder.NewArray())
		record = array.NewRecord(arrow.NewSchema(new_fields, nil), new_columns, int64(tagListBuilder.Len()))
	}

	p.currentRecord = record
	p.currentRecord.Retain()
	return nil
}

func getCsvHeader(input []byte) ([]string, error) {
	reader := csv.NewReader(bytes.NewBuffer(input))
	headers, err := reader.Read()
	if err != nil {
		return nil, errors.New("failed to read header")
	}

	return headers, nil
}
