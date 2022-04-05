package arrow

import (
	"sync"
	"testing"
	"time"

	"github.com/spiceai/data-components-contrib/dataconnectors/flight"
	"github.com/stretchr/testify/assert"
)

func TestCsv(t *testing.T) {
	t.Run("Init()", testInitFunc())
	t.Run("GetRecord()", testProcessor())
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewArrowProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params, nil, nil, nil, nil)
		assert.NoError(t, err)
	}
}

func testProcessor() func(*testing.T) {
	return func(t *testing.T) {
		epoch := time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)
		period := 7 * 24 * time.Hour
		interval := 24 * time.Hour

		var wg sync.WaitGroup

		localFlightConnector := flight.NewFlightConnector()

		err := localFlightConnector.Init(epoch, period, interval, map[string]string{
			"sql": "../../test/assets/data/flight/blocks.sql",
			"key": "3031|abcd",
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

		measurements := map[string]string{}
		categories := map[string]string{}
		dp := NewArrowProcessor()
		err = dp.Init(map[string]string{
			"time_format": "2006-01-02 15:04:05-07:00",
		}, nil, measurements, categories, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(localData)
		assert.NoError(t, err)

		_, err = dp.GetRecord()
		assert.NoError(t, err)
	}
}