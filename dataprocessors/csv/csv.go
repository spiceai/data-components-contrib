package csv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/spiceai/data-components-contrib/dataprocessors/conv"
	"github.com/spiceai/spiceai/pkg/observations"
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
	numTags := len(p.tags)
	if len(p.identifiers)+len(p.measurements)+len(p.categories)+numTags == 0 {
		return nil, nil
	}

	headers, lines, err := getCsvHeaderAndLines(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	timeCol := -1
	tagsCol := make([]int, 0, numTags)
	headersMap := make(map[string]int, len(headers))
	for i, header := range headers {
		headersMap[header] = i
		if timeCol < 0 && header == p.timeSelector {
			timeCol = i
		}
		for _, tag := range p.tags {
			if header == tag {
				tagsCol = append(tagsCol, i)
				break
			}
		}
	}

	if timeCol < 0 {
		return nil, fmt.Errorf("time header '%s' not found", p.timeSelector)
	}

	identifierMappings := getFieldMappings(p.identifiers, headersMap)
	measurementMappings := getFieldMappings(p.measurements, headersMap)
	categoriesMappings := getFieldMappings(p.categories, headersMap)

	var newObservations []observations.Observation
	for line, record := range lines {
		// Process time
		ts, err := spice_time.ParseTime(record[timeCol], p.timeFormat)
		if err != nil {
			log.Printf("ignoring invalid line %d - %v: %v", line+1, record, err)
			continue
		}

		// Process identifiers
		identifiers := map[string]string{}
		for fieldName, col := range identifierMappings {
			identifiers[fieldName] = record[col]
		}

		// Process measurements
		measurements := map[string]float64{}
		for fieldName, col := range measurementMappings {
			field := record[col]

			val, err := conv.ParseMeasurement(field)
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

		// Process tags
		var tags []string
		tagsMap := make(map[string]bool, numTags)
		for _, col := range tagsCol {
			field := record[col]
			for _, tag := range strings.Split(field, " ") {
				if tag != "" && !tagsMap[tag] {
					tags = append(tags, tag)
					tagsMap[tag] = true
				}
			}
		}

		observation := observations.Observation{
			Time: ts.Unix(),
			Tags: tags,
		}

		if len(identifiers) > 0 {
			observation.Identifiers = identifiers
		}

		if len(measurements) > 0 {
			observation.Measurements = measurements
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

	return headers, lines, nil
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
