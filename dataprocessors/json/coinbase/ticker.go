package coinbase

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
)

type Message struct {
	MessageType string    `json:"type,omitempty"`
	Sequence    int64     `json:"sequence,omitempty"`
	ProductId   int64     `json:"product_id,omitempty"`
	Time        time.Time `json:"time,omitempty"`
}

type HeartbeatMessage struct {
	Message
	LastTradeId int64 `json:"last_trade_id,omitempty"`
}

type TickerMessage struct {
	Message
	TradeId  int64   `json:"trade_id,omitempty"`
	Price    float64 `json:"price,omitempty"`
	Side     string  `json:"side,omitempty"`
	LastSize float64 `json:"lastSize,omitempty"`
	BestBid  float64 `json:"bestBid,omitempty"`
	BestAsk  float64 `json:"bestAsk,omitempty"`
}

var (
	//go:embed tweet_schema.json
	jsonSchema []byte
)

type TweetJsonFormat struct {
}

func (s *TweetJsonFormat) GetSchema() []byte {
	return jsonSchema
}

func (s *TweetJsonFormat) GetObservations(data []byte) ([]observations.Observation, error) {
	var tweets []twitter.Tweet

	err := json.Unmarshal(data, &tweets)
	if err != nil {
		return nil, err
	}

	var newObservations []observations.Observation
	for _, tweet := range tweets {
		t, err := tweet.CreatedAtTime()
		if err != nil {
			return nil, fmt.Errorf("tweet time format is invalid: %s", tweet.CreatedAt)
		}

		data := make(map[string]float64)
		data["favorite_count"] = float64(tweet.FavoriteCount)

		tags := []string{tweet.Lang}

		observation := observations.Observation{
			Time: t.Unix(),
			Data: data,
			Tags: tags,
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations, nil
}

func (s *TweetJsonFormat) GetState(data []byte, validFields []string) ([]*state.State, error) {
	// TODO
	return nil, nil
}