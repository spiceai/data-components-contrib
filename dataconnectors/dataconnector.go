package dataconnectors

import (
	"fmt"
	"time"

	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/spiceai/data-components-contrib/dataconnectors/influxdb"
	"github.com/spiceai/data-components-contrib/dataconnectors/twitter"
)

type DataConnector interface {
	Init(Epoch time.Time, Period time.Duration, Interval time.Duration, params map[string]string) error
	Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error
}

func NewDataConnector(name string) (DataConnector, error) {
	switch name {
	case file.FileConnectorName:
		return file.NewFileConnector(), nil
	case influxdb.InfluxDbConnectorName:
		return influxdb.NewInfluxDbConnector(), nil
	case twitter.TwitterConnectorName:
		return twitter.NewTwitterConnector(), nil
	}

	return nil, fmt.Errorf("unknown data connector '%s'", name)
}
