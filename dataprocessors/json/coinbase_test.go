package json

import (
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/spiceai/data-components-contrib/dataconnectors/coinbase"
	"github.com/stretchr/testify/assert"
)

func TestCoinbaseTicker(t *testing.T) {
	dpParams := map[string]string{}

	identifiers := map[string]string{
		"trade_id": "trade_id",
	}

	measurements := map[string]string{
		"price":     "price",
		"last_size": "last_size",
	}

	categories := map[string]string{
		"side": "side",
	}

	dp := NewJsonProcessor()
	err := dp.Init(dpParams, identifiers, measurements, categories, nil)
	if err != nil {
		t.Fatal(err)
	}

	var epoch time.Time
	var period time.Duration
	var interval time.Duration

	params := map[string]string{
		"product_ids": "BTC-USD",
	}

	messageMutex := sync.RWMutex{}
	wg := sync.WaitGroup{}
	wg.Add(5)

	readCount := 0

	dc := coinbase.NewCoinbaseConnector()
	dc.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		messageMutex.Lock()
		defer messageMutex.Unlock()
		if readCount < 5 {
			t.Logf("readCount: %d\n", readCount)
			d, err := dp.OnData(data)
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
	lastSizeCol := record.Column(2).(*array.Float64)
	priceCol := record.Column(3).(*array.Float64)
	for i := 0; i < int(record.NumRows()); i++ {
		assert.Greater(t, timeCol.Value(i), int64(0), "invalid time")
		assert.Greater(t, lastSizeCol.Value(i), 0.0, "slow")
		assert.Greater(t, priceCol.Value(i), 0.0, "normal")
	}
}
