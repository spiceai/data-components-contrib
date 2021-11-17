package coinbase_test

import (
	"fmt"
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
	wg.Add(5)

	var messages [][]byte

	readMutex := sync.Mutex{}
	messageCount := 0
	err := c.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		readMutex.Lock()
		defer readMutex.Unlock()

		if messageCount < 5 {
			fmt.Printf("message: %s\n", string(data))
			messages = append(messages, data)
			defer wg.Done()
			messageCount++
			return nil, nil
		}
		return nil, nil
	})
	assert.NoError(t, err)

	err = c.Init(epoch, period, interval, params)
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}
