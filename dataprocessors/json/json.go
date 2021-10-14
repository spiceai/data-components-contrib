package json

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/api/observation"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/util"
)

const (
	JsonProcessorName string = "json"
)

type JsonProcessor struct {
	measurements map[string]string
	categories   map[string]string

	dataMutex sync.RWMutex
	data      []byte
	dataHash  []byte
}

type JsonFormat interface {
	GetSchema() []byte
	GetObservations(data []byte) ([]observations.Observation, error)
	GetState(data []byte, validFields []string) ([]*state.State, error)
}

type ValidationError struct {
	message         string
	validationError string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s:%s", e.message, e.validationError)
}

func NewJsonProcessor() *JsonProcessor {
	return &JsonProcessor{}
}

func (p *JsonProcessor) Init(params map[string]string, measurements map[string]string, categories map[string]string) error {
	p.measurements = measurements
	p.categories = categories

	return nil
}

func (p *JsonProcessor) OnData(data []byte) ([]byte, error) {
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

func (p *JsonProcessor) GetObservations() ([]observations.Observation, error) {
	if p.data == nil {
		return nil, nil
	}

	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()

	if p.data == nil {
		return nil, nil
	}

	var observationPoints []observation.Observation

	err := json.Unmarshal(p.data, &observationPoints)
	if err != nil {
		return nil, err
	}

	var newObservations []observations.Observation
	for index, point := range observationPoints {
		var ts int64
		var err error

		if point.Time.Integer != nil {
			ts = *point.Time.Integer
		} else if point.Time.String != nil {
			var t time.Time
			t, err = time.Parse(time.RFC3339, *point.Time.String)
			if err != nil {
				// This should never happen as the schema validation would have caught this
				return nil, fmt.Errorf("observation %d time format is invalid: %s", index, *point.Time.String)
			}
			ts = t.Unix()
		} else {
			// This should never happen as the schema validation would have caught this
			return nil, fmt.Errorf("observation %d did not include a time component", index)
		}

		measurements := make(map[string]float64)

		for _, key := range p.measurements {
			if val, ok := point.Data[key]; ok {
				if val.Float64 != nil {
					measurements[key] = *val.Float64
				} else {
					measurements[key], err = strconv.ParseFloat(*val.String, 64)
					if err != nil {
						return nil, err
					}
				}
			}
		}

		categories := make(map[string]string)

		for _, key := range p.categories {
			if val, ok := point.Data[key]; ok {
				categories[key] = *val.String
			}
		}

		observation := observations.Observation{
			Time: ts,
			Tags: point.Tags,
		}

		if len(measurements) > 0 {
			observation.Data = measurements
		}

		if len(categories) > 0 {
			observation.Categories = categories
		}

		newObservations = append(newObservations, observation)
	}

	p.data = nil

	return newObservations, nil
}
