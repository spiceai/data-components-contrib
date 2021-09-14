package json

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/spiceai/data-components-contrib/dataprocessors/json/observation"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/util"
	"go.skia.org/infra/go/jsonschema"
)

const (
	JsonProcessorName string = "json"
)

type JsonProcessor struct {
	data      []byte
	dataMutex sync.RWMutex
	dataHash  []byte
	format    JsonFormat
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
	return e.message
}

func NewJsonProcessor() *JsonProcessor {
	return &JsonProcessor{}
}

func (p *JsonProcessor) Init(params map[string]string) error {
	// Default format if not specified in params
	format := "default"

	if paramFormat, ok := params["format"]; ok {
		format = paramFormat
	}

	switch format {
	case "default":
		p.format = &observation.ObservationJsonFormat{}
	}

	if p.format == nil {
		return fmt.Errorf("unable to find json format '%s'", format)
	}

	return nil
}

func (p *JsonProcessor) OnData(data []byte) ([]byte, error) {
	if p.format == nil {
		return nil, fmt.Errorf("json processor not initialized")
	}

	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	newDataHash, err := util.ComputeNewHash(p.data, p.dataHash, data)
	if err != nil {
		return nil, fmt.Errorf("error computing new data hash in csv processor: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	schemaViolations, err := jsonschema.Validate(ctx, data, *p.format.GetSchema())
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
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()

	if p.data == nil {
		return nil, nil
	}

	observations, err := p.format.GetObservations(&p.data)
	if err != nil {
		return nil, err
	}

	p.data = nil

	return observations, nil
}

func (p *JsonProcessor) GetState(validFields *[]string) ([]*state.State, error) {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()

	if p.data == nil {
		return nil, nil
	}

	state, err := p.format.GetState(&p.data, validFields)
	if err != nil {
		return nil, err
	}

	p.data = nil

	return state, nil
}
