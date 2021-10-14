package coinbase_test

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/spiceai/data-components-contrib/dataconnectors/coinbase"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	c := coinbase.NewCoinbaseConnector()

	var epoch time.Time
	var period time.Duration
	var interval time.Duration

	params := map[string]string{
		"product_ids": "BTC-USD",
	}

	err := c.Init(epoch, period, interval, params)
	assert.NoError(t, err)
}

func TestRead(t *testing.T) {
	c := coinbase.NewCoinbaseConnector()

	var epoch time.Time
	var period time.Duration
	var interval time.Duration

	params := map[string]string{
		"product_ids": "BTC-USD",
	}

	wg := sync.WaitGroup{}
	wg.Add(10)

	var messages [][]byte

	err := c.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		log.Printf("message: %s\n", string(data))
		messages = append(messages, data)
		wg.Done()
		return nil, nil
	})
	assert.NoError(t, err)

	err = c.Init(epoch, period, interval, params)
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}
