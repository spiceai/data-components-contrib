package json

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/util"
	"go.skia.org/infra/go/jsonschema"
)

var (
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

	observations, err := getObservations(observationPoints)
	if err != nil {
		return nil, err
	}

	return observations, nil
}

func getObservations(observationPoints []ObservationPoint) ([]observations.Observation, error) {
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
				return nil, fmt.Errorf("observation time format is invalid: %s", *point.Time.String)
			}
			ts = t.Unix()
		} else {
			// This should never happen as the schema validation would have caught this
			return nil, fmt.Errorf("observation did not include a time component")
		}

		observation := observations.Observation{
			Time: ts,
			Data: point.Data,
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations, nil
}

func (p *JsonProcessor) GetState(validFields *[]string) ([]*state.State, error) {
	return nil, nil
}
