package dataconnectors

import (
	"fmt"
	"time"

	"github.com/phillipleblanc/data-components-contrib/dataconnectors/file"
	"github.com/phillipleblanc/data-components-contrib/dataconnectors/influxdb"
)

type DataConnector interface {
	Init(params map[string]string) error
	FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]byte, error)
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
