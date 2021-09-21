package dataconnectors

import (
	"fmt"

	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/spiceai/data-components-contrib/dataconnectors/influxdb"
)

type DataConnector interface {
	Init(params map[string]string) error
	Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error
}

func NewDataConnector(name string) (DataConnector, error) {
	switch name {
	case file.FileConnectorName:
		return file.NewFileConnector(), nil
	case influxdb.InfluxDbConnectorName:
		return influxdb.NewInfluxDbConnector(), nil
	}

	return nil, fmt.Errorf("unknown data connector '%s'", name)
}
