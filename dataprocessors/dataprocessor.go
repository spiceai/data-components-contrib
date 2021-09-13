package dataprocessors

import (
	"fmt"

	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/data-components-contrib/dataprocessors/flux"
	"github.com/spiceai/data-components-contrib/dataprocessors/json"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
)

type DataProcessor interface {
	Init(params map[string]string) error
	OnData(data []byte) ([]byte, error)
	GetObservations() ([]observations.Observation, error)
	GetState(fields *[]string) ([]*state.State, error)
}

func NewDataProcessor(name string) (DataProcessor, error) {
	switch name {
	case csv.CsvProcessorName:
		return csv.NewCsvProcessor(), nil
	case flux.FluxCsvProcessorName:
		return flux.NewFluxCsvProcessor(), nil
	case json.JsonProcessorName:
		return json.NewJsonProcessor(), nil
	}

	return nil, fmt.Errorf("unknown processor '%s'", name)
}
