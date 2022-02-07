package dataprocessors

import (
	"fmt"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/data-components-contrib/dataprocessors/flux"
	"github.com/spiceai/data-components-contrib/dataprocessors/json"
)

type DataProcessor interface {
	Init(params map[string]string, identifiers map[string]string, measurements map[string]string, categories map[string]string, tags []string) error
	OnData(data []byte) ([]byte, error)
	GetRecord() (arrow.Record, error)
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
