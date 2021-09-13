package json

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/util"
	"go.skia.org/infra/go/jsonschema"
	"go.uber.org/zap"
)

var (
	zaplog *zap.Logger = loggers.ZapLogger()

	//go:embed schema.json
	schema []byte
)

const (
	JsonProcessorName string = "json"
)

type JsonProcessor struct {
	data      []byte
	dataMutex sync.RWMutex
	dataHash  []byte
}

type ValidationError struct {
	message         string
	validationError string
}

func (e *ValidationError) Error() string {
	return e.message
}

func NewJsonProcessor() *JsonProcessor {
	return &JsonProcessor{}
}

func (p *JsonProcessor) Init(params map[string]string) error {
	return nil
}

func (p *JsonProcessor) OnData(data []byte) ([]byte, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	newDataHash, err := util.ComputeNewHash(p.data, p.dataHash, data)
	if err != nil {
		return nil, fmt.Errorf("error computing new data hash in csv processor: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	schemaViolations, err := jsonschema.Validate(ctx, data, schema)
	if err != nil {
		validationError := ""
		if len(schemaViolations) > 0 {
			validationError = schemaViolations[0]
		}

		return nil, &ValidationError{
			message:         err.Error(),
			validationError: validationError,
		}
	}

	if newDataHash != nil {
		// Only update data if new
		p.data = data
		p.dataHash = newDataHash
	}

	return data, nil
}

func (p *JsonProcessor) GetObservations() ([]observations.Observation, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	if p.data == nil {
		return nil, nil
	}

	var observationPoints []ObservationPoint = make([]ObservationPoint, 0)

	err := json.Unmarshal(p.data, &observationPoints)
	if err != nil {
		return nil, err
	}

	p.data = nil
	return getObservations(observationPoints), nil
}

func getObservations(observationPoints []ObservationPoint) []observations.Observation {
	var newObservations []observations.Observation
	for _, point := range observationPoints {
		var ts int64
		var err error

		if point.Time.Integer != nil {
			ts = *point.Time.Integer
		} else if point.Time.String != nil {
			var t time.Time
			t, err = time.Parse(time.RFC3339, *point.Time.String)
			if err != nil {
				// This should never happen as the schema validation would have caught this
				panic(fmt.Sprintf("Observation time format is invalid: %s", *point.Time.String))
			}
			ts = t.Unix()
		} else {
			// This should never happen as the schema validation would have caught this
			panic("Observation did not include a time component")
		}

		observation := observations.Observation{
			Time: ts,
			Data: point.Data,
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations
}

// Processes into State by field path
func (p *JsonProcessor) GetState(validFields *[]string) ([]*state.State, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	reader, err := p.getDataReader()
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, nil
	}

	headers, lines, err := getCsvHeaderAndLines(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	if validFields != nil {
		for i := 1; i < len(headers); i++ {
			header := headers[i]
			fields := *validFields
			found := false
			for _, validField := range fields {
				if validField == header {
					found = true
					break
				}
			}

			if !found {
				return nil, fmt.Errorf("unknown field '%s'", header)
			}
		}
	}

	columnToPath, columnToFieldName, err := getColumnMappings(headers)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	pathToObservations := make(map[string][]observations.Observation)
	pathToFieldNames := make(map[string][]string)

	for col, path := range columnToPath {
		_, ok := pathToObservations[path]
		if !ok {
			pathToObservations[path] = make([]observations.Observation, 0)
			pathToFieldNames[path] = make([]string, 0)
		}
		fieldName := columnToFieldName[col]
		pathToFieldNames[path] = append(pathToFieldNames[path], fieldName)
	}

	zaplog.Sugar().Debugf("Read headers of %v", headers)

	numDataFields := len(headers) - 1

	for line, record := range lines {
		ts, err := util.ParseTime(record[0])
		if err != nil {
			log.Printf("ignoring invalid line %d - %v: %v", line+1, record, err)
			continue
		}

		lineData := make(map[string]map[string]float64, numDataFields)

		for col := 1; col < len(record); col++ {
			field := record[col]

			if field == "" {
				continue
			}

			val, err := strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("ignoring invalid field %d - %v: %v", line+1, field, err)
				continue
			}

			fieldCol := col - 1
			fieldName := columnToFieldName[fieldCol]

			path := columnToPath[fieldCol]
			data := lineData[path]
			if data == nil {
				data = make(map[string]float64)
				lineData[path] = data
			}

			data[fieldName] = val
		}

		if len(lineData) == 0 {
			continue
		}

		for path, data := range lineData {
			observation := &observations.Observation{
				Time: ts,
				Data: data,
			}
			obs := pathToObservations[path]
			pathToObservations[path] = append(obs, *observation)
		}
	}

	result := make([]*state.State, len(pathToObservations))

	i := 0
	for path, obs := range pathToObservations {
		fieldNames := pathToFieldNames[path]
		result[i] = state.NewState(path, fieldNames, obs)
		i++
	}

	p.data = nil
	return result, nil
}

func (p *JsonProcessor) getDataReader() (io.Reader, error) {
	if p.data == nil {
		return nil, nil
	}

	reader := bytes.NewReader(p.data)
	return reader, nil
}

func getCsvHeaderAndLines(input io.Reader) ([]string, [][]string, error) {
	reader := csv.NewReader(input)
	headers, err := reader.Read()
	if err != nil {
		return nil, nil, errors.New("failed to read header")
	}

	lines, err := reader.ReadAll()
	if err != nil {
		return nil, nil, errors.New("failed to read lines")
	}

	if len(headers) <= 1 || len(lines) == 0 {
		return nil, nil, errors.New("no data")
	}

	// Temporary restriction until mapped fields are supported
	if headers[0] != "time" {
		return nil, nil, errors.New("first column must be 'time'")
	}

	return headers, lines, nil
}

// Returns mapping of column index to path and field name
func getColumnMappings(headers []string) ([]string, []string, error) {
	numDataFields := len(headers) - 1

	columnToPath := make([]string, numDataFields)
	columnToFieldName := make([]string, numDataFields)

	for i := 1; i < len(headers); i++ {
		header := headers[i]
		dotIndex := strings.LastIndex(header, ".")
		if dotIndex == -1 {
			return nil, nil, fmt.Errorf("header '%s' expected to be full-qualified", header)
		}
		columnToPath[i-1] = header[:dotIndex]
		columnToFieldName[i-1] = header[dotIndex+1:]
	}

	return columnToPath, columnToFieldName, nil
}
