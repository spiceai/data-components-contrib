package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/spiceai/data-components-contrib/dataprocessors/conv"

	// "github.com/spiceai/spiceai/pkg/observations"
	spice_time "github.com/spiceai/spiceai/pkg/time"
	"github.com/spiceai/spiceai/pkg/util"
)

const (
	JsonProcessorName string = "json"
)

type JsonProcessor struct {
	timeFormat   string
	timeSelector string

	timeBuilder     *array.Int64Builder
	idColNames      []string
	idFields        map[string]arrow.Field
	idBuilders      map[string]*array.StringBuilder
	measureColNames []string
	measureFields   map[string]arrow.Field
	measureBuilders map[string]*array.Float64Builder
	catColNames     []string
	catFields       map[string]arrow.Field
	catBuilders     map[string]*array.StringBuilder
	tags            []string
	tagBuilder      *array.ListBuilder

	dataMutex    sync.RWMutex
	data         [][]byte
	lastDataHash []byte
}

func NewJsonProcessor() *JsonProcessor {
	return &JsonProcessor{}
}

func (p *JsonProcessor) Init(params map[string]string, identifiers map[string]string, measurements map[string]string, categories map[string]string, tags []string) error {
	if val, ok := params["time_format"]; ok {
		p.timeFormat = val
	}
	if selector, ok := params["time_selector"]; ok && selector != "" {
		p.timeSelector = selector
	} else {
		p.timeSelector = "time"
	}

	p.idFields = make(map[string]arrow.Field)
	p.idBuilders = make(map[string]*array.StringBuilder)
	p.measureFields = make(map[string]arrow.Field)
	p.measureBuilders = make(map[string]*array.Float64Builder)
	p.catFields = make(map[string]arrow.Field)
	p.catBuilders = make(map[string]*array.StringBuilder)

	for colName, fieldName := range identifiers {
		p.idColNames = append(p.idColNames, colName)
		p.idFields[colName] = arrow.Field{Name: fmt.Sprintf("id.%s", fieldName), Type: arrow.BinaryTypes.String}
	}
	sort.Strings(p.idColNames)
	for colName, fieldName := range measurements {
		p.measureColNames = append(p.measureColNames, colName)
		p.measureFields[colName] = arrow.Field{Name: fmt.Sprintf("measure.%s", fieldName), Type: arrow.PrimitiveTypes.Float64}
	}
	sort.Strings(p.measureColNames)
	for colName, fieldName := range categories {
		p.catColNames = append(p.catColNames, colName)
		p.catFields[colName] = arrow.Field{Name: fmt.Sprintf("cat.%s", fieldName), Type: arrow.BinaryTypes.String}
	}
	sort.Strings(p.catColNames)
	p.tags = tags

	return nil
}

func (p *JsonProcessor) OnData(data []byte) ([]byte, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	var currentData []byte
	if p.data != nil && len(p.data) > 0 {
		currentData = p.data[len(p.data)-1]
	}

	newDataHash, err := util.ComputeNewHash(currentData, p.lastDataHash, data)
	if err != nil {
		return nil, fmt.Errorf("error computing new data hash in json processor: %w", err)
	}

	if newDataHash != nil {
		// Only update data if new
		p.data = append(p.data, data)
		p.lastDataHash = newDataHash
	}

	return data, nil
}

func (p *JsonProcessor) GetObservations() (array.Record, error) {
	if p.data == nil {
		return nil, nil
	}

	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()

	if p.data == nil {
		return nil, nil
	}

	// Builders creation
	pool := memory.NewGoAllocator()

	fields := []arrow.Field{{Name: "time", Type: arrow.PrimitiveTypes.Int64}}

	p.timeBuilder = array.NewInt64Builder(pool)
	defer p.timeBuilder.Release()
	for _, colName := range p.idColNames {
		p.idBuilders[colName] = array.NewStringBuilder(pool)
		fields = append(fields, p.idFields[colName])
	}
	for _, colName := range p.measureColNames {
		p.measureBuilders[colName] = array.NewFloat64Builder(pool)
		fields = append(fields, p.measureFields[colName])
	}
	for _, colName := range p.catColNames {
		p.catBuilders[colName] = array.NewStringBuilder(pool)
		fields = append(fields, p.catFields[colName])
	}
	p.tagBuilder = array.NewListBuilder(pool, arrow.BinaryTypes.String)
	fields = append(fields, arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)})
	defer p.tagBuilder.Release()

	for _, data := range p.data {
		firstChar := string(data[:1])
		if firstChar == "{" {
			var item map[string]json.RawMessage

			err := json.Unmarshal(data, &item)
			if err != nil {
				return nil, err
			}
			err = p.newObservationFromJson(0, item)
			if err != nil {
				return nil, fmt.Errorf("error unmarshaling item: %s", err.Error())
			}
		} else {
			var items []map[string]json.RawMessage

			err := json.Unmarshal(data, &items)
			if err != nil {
				return nil, err
			}

			for index, item := range items {
				err = p.newObservationFromJson(index, item)
				if err != nil {
					return nil, fmt.Errorf("error unmarshaling item %d: %s", index, err.Error())
				}
			}
		}
	}

	p.data = nil

	cols := []array.Interface{p.timeBuilder.NewArray()}
	for _, colName := range p.idColNames {
		cols = append(cols, p.idBuilders[colName].NewArray())
	}
	for _, colName := range p.measureColNames {
		cols = append(cols, p.measureBuilders[colName].NewArray())
	}
	for _, colName := range p.catColNames {
		cols = append(cols, p.catBuilders[colName].NewArray())
	}
	cols = append(cols, p.tagBuilder.NewArray())

	record := array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(cols[0].Len()))
	record.Retain()

	// Builders release
	for _, builder := range p.idBuilders {
		defer builder.Release()
	}
	for _, builder := range p.measureBuilders {
		defer builder.Release()
	}
	for _, builder := range p.catBuilders {
		defer builder.Release()
	}

	return record, nil
}

func (p *JsonProcessor) newObservationFromJson(index int, item map[string]json.RawMessage) error {
	timeEntry, ok := item[p.timeSelector]
	if !ok {
		return fmt.Errorf("time field with selector '%s' does not exist in the message", p.timeSelector)
	}

	timeValue, err := unmarshalTime(p.timeFormat, timeEntry)
	if err != nil {
		return err
	}
	p.timeBuilder.Append(timeValue.Unix())

	for colName, field := range p.idFields {
		if val, ok := item[field.Name[3:]]; ok { // Field name starts with "id."
			var jsonVal interface{}
			err = json.Unmarshal(val, &jsonVal)
			if err != nil {
				return err
			}
			if stringValue, ok := jsonVal.(string); ok {
				p.idBuilders[colName].Append(stringValue)
			} else if numValue, ok := jsonVal.(float64); ok {
				p.idBuilders[colName].Append(strconv.FormatFloat(numValue, 'f', -1, 64))
			} else {
				return fmt.Errorf("identifier field '%s' is not a a valid id (string or number)", colName)
			}
		} else {
			p.idBuilders[colName].AppendNull()
		}
	}

	for colName, field := range p.measureFields {
		if val, ok := item[field.Name[8:]]; ok { // Field name starts with "measure."
			var numValue float64
			err = json.Unmarshal(val, &numValue)
			if err != nil {
				var str string
				strErr := json.Unmarshal(val, &str)
				if strErr != nil {
					return err
				}
				numValue, err = conv.ParseMeasurement(str)
				if err != nil {
					return err
				}
			}
			p.measureBuilders[colName].Append(numValue)
		} else {
			p.measureBuilders[colName].AppendNull()
		}
	}

	for colName, field := range p.catFields {
		if val, ok := item[field.Name[4:]]; ok { // Field name starts with "cat."
			stringValue, err := unmarshalString(val)
			if err != nil {
				return err
			}
			p.catBuilders[colName].Append(stringValue)
		} else {
			p.catBuilders[colName].AppendNull()
		}
	}

	p.tagBuilder.Append(true)
	tagAdded := make(map[string]bool)
	tagValueBuilder := p.tagBuilder.ValueBuilder().(*array.StringBuilder)

	for _, colName := range p.tags {
		tagEntry, ok := item[colName]
		if !ok {
			continue
		}

		if colName == "_tags" || colName == "tags" {
			var stringList []string
			err = json.Unmarshal(tagEntry, &stringList)
			if err != nil {
				return err
			}
			for _, stringValue := range stringList {
				// Avoid duplicate entries
				if _, ok := tagAdded[stringValue]; !ok {
					tagValueBuilder.Append(stringValue)
					tagAdded[stringValue] = true
				}
			}
			continue
		}

		var stringValue string
		err = json.Unmarshal(tagEntry, &stringValue)
		if err != nil {
			return err
		}
		if _, ok := tagAdded[stringValue]; !ok {
			tagValueBuilder.Append(stringValue)
			tagAdded[stringValue] = true
		}
	}

	return nil
}

func unmarshalTime(timeFormat string, data []byte) (*time.Time, error) {
	st := spice_time.Time{}
	err := st.UnmarshalJSON(data)
	if err != nil {
		return nil, fmt.Errorf("time format is invalid: %s", string(data))
	}

	if st.Integer != nil {
		t := time.Unix(*st.Integer, 0)
		return &t, nil
	}

	if st.String != nil {
		var t time.Time
		t, err = spice_time.ParseTime(*st.String, timeFormat)
		if err != nil {
			return nil, fmt.Errorf("time format is invalid: %s", *st.String)
		}
		return &t, nil
	}

	return nil, errors.New("did not include a time component")
}

func unmarshalString(val json.RawMessage) (string, error) {
	var str string
	err := json.Unmarshal(val, &str)
	if err == nil {
		return str, err
	}
	var i int64
	err = json.Unmarshal(val, &i)
	if err == nil {
		return strconv.FormatInt(i, 10), err
	}
	var f float64
	err = json.Unmarshal(val, &f)
	if err == nil {
		return strconv.FormatFloat(f, 'f', -1, 64), err
	}
	return "", fmt.Errorf("value is not a valid string or number")
}
