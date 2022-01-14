package json

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/spiceai/data-components-contrib/dataconnectors/http"
	"github.com/stretchr/testify/assert"
)

func TestGasFeesTicker(t *testing.T) {
	dpParams := map[string]string{}

	identifiers := map[string]string{
		"lastBlock": "lastBlock",
	}

	measurements := map[string]string{
		"nextBlockBaseFee": "nextBlockBaseFee",
		"slow":             "slow",
		"normal":           "normal",
		"fast":             "fast",
		"instant":          "instant",
	}

	dp := NewJsonProcessor()
	err := dp.Init(dpParams, identifiers, measurements, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	var epoch time.Time
	var period time.Duration
	var interval time.Duration

	params := map[string]string{
		"url":              "https://data.spiceai.io/eth/v0.1/gasfees",
		"timeout":          "2s",
		"polling_interval": "1s",
	}

	messageMutex := sync.RWMutex{}
	wg := sync.WaitGroup{}
	wg.Add(5)

	readCount := 0

	dc := http.NewHttpConnector()
	dc.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		messageMutex.Lock()
		defer messageMutex.Unlock()
		if readCount < 5 {
			t.Logf("readCount: %d\n", readCount)
			d, err := dp.OnData(data)
			fmt.Println(string(d))
			readCount++
			wg.Done()
			return d, err
		}
		readCount++
		return nil, nil
	})

	err = dc.Init(epoch, period, interval, params)
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	record, err := dp.GetObservations()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, int(record.NumRows()), 5)

	timeCol := record.Column(0).(*array.Int64)
	fastCol := record.Column(2).(*array.Float64)
	instantCol := record.Column(3).(*array.Float64)
	normalCol := record.Column(5).(*array.Float64)
	slowCol := record.Column(6).(*array.Float64)
	for i := 0; i < int(record.NumRows()); i++ {
		assert.Greater(t, timeCol.Value(i), int64(0), "invalid time")
		assert.Greater(t, slowCol.Value(i), 0.0, "slow")
		assert.Greater(t, normalCol.Value(i), 0.0, "normal")
		assert.Greater(t, fastCol.Value(i), 0.0, "fast")
		assert.Greater(t, instantCol.Value(i), 0.0, "instant")
	}
}
