package csv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"strings"

	"sync"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	arrow_csv "github.com/apache/arrow/go/v7/arrow/csv"
	"github.com/apache/arrow/go/v7/arrow/memory"

	spice_time "github.com/spiceai/spiceai/pkg/time"
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

	currentRecord arrow.Record
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

func (p *CsvProcessor) GetRecord() (arrow.Record, error) {
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

	err := p.createRecord(p.data)
	if err != nil {
		return nil, err
	}

	p.data = nil
	return p.currentRecord, nil
}

func (p *CsvProcessor) createRecord(data []byte) error {
	numTags := len(p.tags)

	if len(p.identifiers)+len(p.measurements)+len(p.categories)+numTags == 0 {
		return nil
	}

	identifiersMap := make(map[string]string)
	measurementsMap := make(map[string]string)
	categoriesMap := make(map[string]string)
	tagsMap := make(map[string]bool)
	for key, colName := range p.identifiers {
		identifiersMap[colName] = key
	}
	for key, colName := range p.measurements {
		measurementsMap[colName] = key
	}
	for key, colName := range p.categories {
		categoriesMap[colName] = key
	}
	for _, key := range p.tags {
		tagsMap[key] = true
	}

	headers, err := getCsvHeader(data)
	if err != nil {
		return fmt.Errorf("failed to get csv header: %s", err)
	}

	fields := make([]arrow.Field, len(headers))
	var colToInclude []int
	includeAllColumns := true
	var tagColumns []int
	timeCol := -1
	for i, header := range headers {
		// Allocate time column index upon finding timeSelector if not yet assigned
		if timeCol < 0 && header == p.timeSelector {
			timeCol = i
			fields[i].Name = header
			if p.timeFormat != "" {
				fields[i].Type = arrow.BinaryTypes.String
			} else {
				fields[i].Type = arrow.PrimitiveTypes.Int64
			}
			colToInclude = append(colToInclude, i)
		} else {
			if key, ok := identifiersMap[header]; ok {
				fields[i].Name = fmt.Sprintf("id.%s", key)
				fields[i].Type = arrow.BinaryTypes.String
				colToInclude = append(colToInclude, i)
			} else if key, ok := measurementsMap[header]; ok {
				fields[i].Name = fmt.Sprintf("measure.%s", key)
				fields[i].Type = arrow.PrimitiveTypes.Float64
				colToInclude = append(colToInclude, i)
			} else if key, ok := categoriesMap[header]; ok {
				fields[i].Name = fmt.Sprintf("cat.%s", key)
				fields[i].Type = arrow.BinaryTypes.String
				colToInclude = append(colToInclude, i)
			} else if tagsMap[header] {
				tagColumns = append(tagColumns, i)
				fields[i].Name = fmt.Sprintf("tag.%s", header)
				fields[i].Type = arrow.BinaryTypes.String
				colToInclude = append(colToInclude, i)
			} else {
				fields[i].Name = fmt.Sprintf("ignore.%s", header)
				fields[i].Type = arrow.BinaryTypes.String
				includeAllColumns = false
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
	if !includeAllColumns {
		var newFields []arrow.Field
		var newColumns []arrow.Array
		var newTagColumns []int
		for newIndex, oldIndex := range colToInclude {
			if fields[oldIndex].Name[:4] == "tag." {
				newTagColumns = append(newTagColumns, newIndex)
			}
			newFields = append(newFields, fields[oldIndex])
			newColumns = append(newColumns, record.Column(oldIndex))
		}
		fields = newFields
		tagColumns = newTagColumns
		record = array.NewRecord(arrow.NewSchema(fields, nil), newColumns, record.NumRows())
	}

	pool := memory.NewGoAllocator()
	columns := record.Columns()
	if p.timeFormat != "" {
		timeBuilder := array.NewInt64Builder(pool)
		tagCol := record.Columns()[timeCol].(*array.String)
		for i := 0; i < int(record.NumRows()); i++ {
			time, err := spice_time.ParseTime(tagCol.Value(i), p.timeFormat)
			if err != nil {
				log.Printf("ignoring invalid line %d (%v): %v", i+1, tagCol.Value(i), err)
				timeBuilder.Append(0)
				continue
			} else {
				timeBuilder.Append(time.Unix())
			}
		}
		fields = append([]arrow.Field{{Name: "time", Type: arrow.PrimitiveTypes.Int64}}, fields[1:]...)
		columns = append([]arrow.Array{timeBuilder.NewArray()}, record.Columns()[1:]...)
		record = array.NewRecord(arrow.NewSchema(fields, nil), columns, record.NumRows())
	}

	// Aggregating tags
	tagListBuilder := array.NewListBuilder(pool, arrow.BinaryTypes.String)
	tagValueBuilder := tagListBuilder.ValueBuilder().(*array.StringBuilder)
	defer tagListBuilder.Release()

	if len(tagColumns) > 0 {
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
		var newFields []arrow.Field
		var newColumns []arrow.Array
		for i, field := range fields {
			if field.Name[:4] != "tag." {
				newFields = append(newFields, field)
				newColumns = append(newColumns, record.Column(i))
			}
		}
		fields = newFields
		columns = newColumns
	} else {
		for i := int64(0); i < record.NumRows(); i++ {
			tagListBuilder.Append(true)
		}
	}
	fields = append(fields, arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)})
	columns = append(columns, tagListBuilder.NewArray())
	record = array.NewRecord(arrow.NewSchema(fields, nil), columns, record.NumRows())

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
