package csv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/time"
	"github.com/spiceai/spiceai/pkg/util"
	"go.uber.org/zap"
)

var (
	zaplog *zap.Logger = loggers.ZapLogger()
)

const (
	CsvProcessorName string = "csv"
	tagsColumnName   string = "_tags"
)

type CsvProcessor struct {
	timeFormat string

	measurements map[string]string
	categories   map[string]string

	dataMutex sync.RWMutex
	data      []byte
	dataHash  []byte
}

func NewCsvProcessor() *CsvProcessor {
	return &CsvProcessor{}
}

func (p *CsvProcessor) Init(params map[string]string, measurements map[string]string, categories map[string]string) error {
	if format, ok := params["time_format"]; ok {
		p.timeFormat = format
	}

	p.measurements = measurements
	p.categories = categories

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

func (p *CsvProcessor) GetObservations() ([]observations.Observation, error) {
	if p.data == nil {
		return nil, nil
	}

	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	if p.data == nil {
		return nil, nil
	}

	reader := bytes.NewReader(p.data)
	if reader == nil {
		return nil, nil
	}

	newObservations, err := p.getObservations(reader)
	if err != nil {
		return nil, err
	}

	p.data = nil
	return newObservations, nil
}

func (p *CsvProcessor) getObservations(reader io.Reader) ([]observations.Observation, error) {
	if len(p.measurements)+len(p.categories) == 0 {
		return nil, nil
	}

	headers, lines, err := getCsvHeaderAndLines(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	headersMap := make(map[string]int, len(headers))
	for i, header := range headers {
		headersMap[header] = i
	}

	tagsColumn := -1
	for i, header := range headers {
		if header == tagsColumnName {
			tagsColumn = i
			break
		}
	}

	measurementMappings := getFieldMappings(p.measurements, headersMap)
	categoriesMappings := getFieldMappings(p.categories, headersMap)

	var newObservations []observations.Observation
	for line, record := range lines {
		// Process time
		ts, err := time.ParseTime(record[0], p.timeFormat)
		if err != nil {
			log.Printf("ignoring invalid line %d - %v: %v", line+1, record, err)
			continue
		}

		// Process tags
		var tags []string
		if tagsColumn >= 0 {
			tags = strings.Split(record[tagsColumn], " ")
		}

		// Process measurements
		measurements := map[string]float64{}
		for fieldName, col := range measurementMappings {
			field := record[col]

			val, err := strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("ignoring invalid field %d - %v: %v", line+1, field, err)
				continue
			}

			measurements[fieldName] = val
		}

		// Process categories
		categories := map[string]string{}
		for fieldName, col := range categoriesMappings {
			categories[fieldName] = record[col]
		}

		observation := observations.Observation{
			Time: ts.Unix(),
			Tags: tags,
		}

		if len(measurements) > 0 {
			observation.Data = measurements
		}

		if len(categories) > 0 {
			observation.Categories = categories
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations, nil
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

func getFieldMappings(fields map[string]string, headers map[string]int) map[string]int {
	mappings := make(map[string]int, len(fields))

	for fieldName, dataName := range fields {
		if val, ok := headers[dataName]; ok {
			mappings[fieldName] = val
		}
	}

	return mappings
}
