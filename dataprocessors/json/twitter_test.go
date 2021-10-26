package json

import (
	"sync"
	"testing"
	"time"

	"github.com/spiceai/data-components-contrib/dataconnectors/twitter"
	"github.com/stretchr/testify/assert"
)

func TestTwitterTweets(t *testing.T) {
	processorParams := map[string]string{
		"time_format":   "Mon Jan 02 15:04:05 -0700 2006",
		"time_selector": "created_at",
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
	err := dp.Init(processorParams, measurements, categories)
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

	observations, err := dp.GetObservations()
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, observations, 5)

	for _, o := range observations {
		assert.Greater(t, o.Time, time.Now().Add(-time.Second*5), "invalid time")
		assert.GreaterOrEqual(t, o.Measurements["favorite_count"], 0.0, "invalid favorite_count")
		assert.GreaterOrEqual(t, o.Measurements["quote_count"], 0.0, "invalid quote_count")
		assert.GreaterOrEqual(t, o.Measurements["reply_count"], 0.0, "invalid reply_count")
		assert.NotEmpty(t, o.Categories["lang"], "invalid lang")
		assert.NotEmpty(t, o.Categories["filter_level"], "invalid filter_level")
	}
}
