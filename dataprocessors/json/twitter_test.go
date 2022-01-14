package json

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/spiceai/data-components-contrib/dataconnectors/twitter"
	"github.com/stretchr/testify/assert"
)

func TestTwitterTweets(t *testing.T) {
	processorParams := map[string]string{
		"time_format":   "Mon Jan 02 15:04:05 -0700 2006",
		"time_selector": "created_at",
	}

	identifiers := map[string]string{
		"id": "id_str",
	}

	measurements := map[string]string{
		"favorite_count": "favorite_count",
		"quote_count":    "quote_count",
		"reply_count":    "reply_count",
		"retweet_count":  "retweet_count",
	}

	categories := map[string]string{
		"lang":         "lang",
		"filter_level": "filter_level",
	}

	dp := NewJsonProcessor()
	err := dp.Init(processorParams, identifiers, measurements, categories, nil)
	if err != nil {
		t.Fatal(err)
	}

	var epoch time.Time
	var period time.Duration
	var interval time.Duration

	connectorParams := map[string]string{
		"consumer_key":    "change_me",
		"consumer_secret": "change_me",
		"access_token":    "change_me",
		"access_secret":   "change_me",
		"filter":          "i",
	}

	if connectorParams["consumer_key"] == "change_me" {
		t.SkipNow()
	}

	messageMutex := sync.RWMutex{}
	wg := sync.WaitGroup{}
	wg.Add(5)

	readCount := 0

	dc := twitter.NewTwitterConnector()
	dc.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		messageMutex.Lock()
		defer messageMutex.Unlock()
		if readCount < 5 {
			d, err := dp.OnData(data)
			wg.Done()
			readCount++
			return d, err
		}
		readCount++
		return nil, nil
	})

	err = dc.Init(epoch, period, interval, connectorParams)
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	record, err := dp.GetObservations()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, int(record.NumRows()), 5)

	fmt.Println(record)

	timeCol := record.Column(0).(*array.Int64)
	idCol := record.Column(1).(*array.String)
	for i := 0; i < int(record.NumRows()); i++ {
		// 30 seconds buffer for clock skew
		assert.Greater(t, timeCol.Value(i), time.Now().Add(-time.Second*30).Unix(), "invalid time")
		assert.NotEmpty(t, idCol.Value(i), "invalid id")
	}

	// for _, o := range observations {
	// 	assert.Greater(t, o.Time, time.Now().Add(-time.Second*30).Unix(), "invalid time") // 30 seconds buffer for clock skew
	// 	assert.NotEmpty(t, o.Identifiers["id"], "invalid id")
	// 	assert.GreaterOrEqual(t, o.Measurements["favorite_count"], 0.0, "invalid favorite_count")
	// 	assert.GreaterOrEqual(t, o.Measurements["quote_count"], 0.0, "invalid quote_count")
	// 	assert.GreaterOrEqual(t, o.Measurements["reply_count"], 0.0, "invalid reply_count")
	// 	assert.NotEmpty(t, o.Categories["lang"], "invalid lang")
	// 	assert.NotEmpty(t, o.Categories["filter_level"], "invalid filter_level")
	// }
}
