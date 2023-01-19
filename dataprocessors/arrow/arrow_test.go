package arrow

import (
	"sync"
	"testing"
	"time"

	"github.com/spiceai/data-components-contrib/dataconnectors/flight"
	"github.com/stretchr/testify/assert"
)

const (
	SPICE_XYZ_API_KEY = "323431|d7a8daf65b334509974fb3a1c9ec6c6d"
)

func TestInit(t *testing.T) {
	p := NewArrowProcessor()

	params := map[string]string{}

	err := p.Init(params, nil, nil, nil, nil)
	assert.NoError(t, err)
}

func TestProcessor(t *testing.T) {
	epoch := time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)
	period := 7 * 24 * time.Hour
	interval := 24 * time.Hour

	var wg sync.WaitGroup

	localFlightConnector := flight.NewFlightConnector()

	err := localFlightConnector.Init(epoch, period, interval, map[string]string{
		"sql":      "../../test/assets/data/flight/blocks.sql",
		"password": SPICE_XYZ_API_KEY,
		"url":      "flight.spiceai.io:443",
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	var localData []byte
	wg.Add(1)
	err = localFlightConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		localData = data
		wg.Done()
		return nil, nil
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(localData) == 0 {
		t.Fatal("no data")
	}

	measurements := map[string]string{
		"gas-limit": "gas_limit",
	}
	categories := map[string]string{}
	dp := NewArrowProcessor()
	err = dp.Init(map[string]string{"time_selector": "timestamp"}, nil, measurements, categories, nil)
	assert.NoError(t, err)

	_, err = dp.OnData(localData)
	assert.NoError(t, err)

	_, err = dp.GetRecord()
	assert.NoError(t, err)
}
