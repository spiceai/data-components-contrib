package file_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataconnectors/file"))

func TestFileConnector(t *testing.T) {
	filesToTest := []string{"COINBASE_BTCUSD, 30.csv"}
	for _, fileToTest := range filesToTest {
		filePath := filepath.Join("../../test/assets/data/csv", fileToTest)

		params := make(map[string]string)
		params["path"] = filePath
		params["watch"] = "false"

		t.Run(fmt.Sprintf("Init() - %s", fileToTest), testInitFunc(params))
		t.Run(fmt.Sprintf("Read() - %s", fileToTest), testReadFunc(params))
	}
}

func testInitFunc(params map[string]string) func(*testing.T) {
	c := file.NewFileConnector()

	return func(t *testing.T) {
		var epoch time.Time
		var period time.Duration
		var interval time.Duration

		err := c.Init(epoch, period, interval, params)
		assert.NoError(t, err)
	}
}

func testReadFunc(params map[string]string) func(*testing.T) {
	c := file.NewFileConnector()

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

		assert.Equal(t, "82627", readMetadata["size"])

		modTime, err := time.Parse(time.RFC3339Nano, readMetadata["mod_time"])
		assert.NoError(t, err)
		assert.Equal(t, int64(1632218687313302691), modTime.UnixNano())

		snapshotter.SnapshotT(t, string(readData))
	}
}
