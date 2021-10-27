package twitter_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	spice_twitter "github.com/spiceai/data-components-contrib/dataconnectors/twitter"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	c := spice_twitter.NewTwitterConnector()

	var epoch time.Time
	var period time.Duration
	var interval time.Duration

	params := getAuthParams()
	if len(params["consumer_key"]) < 25 {
		t.SkipNow()
	}
	params["filter"] = "hodl"

	err := c.Init(epoch, period, interval, params)
	assert.NoError(t, err)
}

func TestRead(t *testing.T) {
	c := spice_twitter.NewTwitterConnector()

	var epoch time.Time
	var period time.Duration
	var interval time.Duration

	params := getAuthParams()
	if len(params["consumer_key"]) < 25 {
		t.SkipNow()
	}
	params["filter"] = "I"

	wg := sync.WaitGroup{}
	wg.Add(5)

	var allTweets []twitter.Tweet
	tweetsMutex := sync.RWMutex{}

	err := c.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		if assert.Equal(t, metadata["type"], "tweet") {
			var tweets []twitter.Tweet
			err := json.Unmarshal(data, &tweets)
			if assert.NoError(t, err) {
				tweetsMutex.Lock()
				for _, tweet := range tweets {
					if len(tweets) > 5 {
						continue
					}
					t.Logf("tweet: %s\n", tweet.Text)
					allTweets = append(allTweets, tweet)
					wg.Done()
				}

				tweetsMutex.Unlock()
			}
		}
		return nil, nil
	})
	assert.NoError(t, err)

	err = c.Init(epoch, period, interval, params)
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	assert.Len(t, allTweets, 5)
}

func getAuthParams() map[string]string {
	return map[string]string{
		"consumer_key":    "change_me",
		"consumer_secret": "change_me",
		"access_token":    "change_me",
		"access_secret":   "change_me",
	}
}
