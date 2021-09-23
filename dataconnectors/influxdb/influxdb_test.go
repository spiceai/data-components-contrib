package influxdb_test

import (
	"testing"
	"time"

	"github.com/spiceai/data-components-contrib/dataconnectors/influxdb"
	"github.com/stretchr/testify/assert"
)

func TestInfluxDbConnector(t *testing.T) {
	params := map[string]string{
		"url":   "fake-url-for-test",
		"token": "fake-token-for-test",
	}

	t.Run("Init()", testInitFunc(params))
}

func testInitFunc(params map[string]string) func(*testing.T) {
	c := influxdb.NewInfluxDbConnector()

	return func(t *testing.T) {		
		var epoch time.Time
		var period time.Duration
		var interval time.Duration

		err := c.Init(epoch, period, interval, params)
		assert.NoError(t, err)
	}
}
