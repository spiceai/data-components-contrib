package http_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/http"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataconnectors/http"))

func TestHttpConnector(t *testing.T) {
	urlsToTest := []string{"https://data.spiceai.io/health"}
	for _, urlToTest := range urlsToTest {
		params := make(map[string]string)
		params["url"] = urlToTest
		params["timeout"] = "2s"

		t.Run(fmt.Sprintf("Init() - %s", urlToTest), testInitFunc(params))
		t.Run(fmt.Sprintf("Read() - %s", urlToTest), testReadFunc(params))
	}
}

func testInitFunc(params map[string]string) func(*testing.T) {
	c := http.NewHttpConnector()

	return func(t *testing.T) {
		var epoch time.Time
		var period time.Duration
		var interval time.Duration

		err := c.Init(epoch, period, interval, params)
		assert.NoError(t, err)
	}
}

func testReadFunc(params map[string]string) func(*testing.T) {
	c := http.NewHttpConnector()

	return func(t *testing.T) {
		var readData []byte
		var readMetadata map[string]string

		readChan := make(chan bool, 1)

		err := c.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
			readData = data
			readMetadata = metadata
			readChan <- true
			return nil, nil
		})
		assert.NoError(t, err)

		var epoch time.Time
		var period time.Duration
		var interval time.Duration

		err = c.Init(epoch, period, interval, params)
		assert.NoError(t, err)

		<-readChan

		assert.Equal(t, "200", readMetadata["status_code"])
		assert.Equal(t, "200 OK", readMetadata["status"])
		assert.Equal(t, "3", readMetadata["content_length"])

		_, err = time.Parse(time.RFC3339Nano, readMetadata["time"])
		assert.NoError(t, err)

		assert.Equal(t, "ok", strings.TrimSpace(string(readData)))
	}
}
