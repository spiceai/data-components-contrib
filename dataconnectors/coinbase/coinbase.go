package coinbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/logrusorgru/aurora"
	"golang.org/x/sync/errgroup"
)

type SubscribeRequest struct {
	RequestType string   `json:"type,omitempty"`
	ProductIds  []string `json:"product_ids,omitempty"`
	Channels    []string `json:"channels,omitempty"`
}

type MessageHeaders struct {
	MessageType string `json:"type,omitempty"`
	Sequence    *int   `sequence:"type,omitempty"`
}

type Subscriptions struct {
	Type     string    `json:"type"`
	Channels []Channel `json:"channels"`
}

type Channel struct {
	Name       string   `json:"name"`
	ProductIDS []string `json:"product_ids"`
}

type Heartbeat struct {
	Type        string `json:"type"`
	LastTradeID int64  `json:"last_trade_id"`
	ProductID   string `json:"product_id"`
	Sequence    int64  `json:"sequence"`
	Time        string `json:"time"`
}

const (
	CoinbaseConnectorName string = "coinbase"
)

type CoinbaseConnector struct {
	readHandlers []*func(data []byte, metadata map[string]string) ([]byte, error)
}

func NewCoinbaseConnector() *CoinbaseConnector {
	return &CoinbaseConnector{}
}

func (c *CoinbaseConnector) Init(epoch time.Time, period time.Duration, interval time.Duration, params map[string]string) error {
	pids := params["product_ids"]
	if pids == "" {
		return errors.New("product_ids is required")
	}

	channels := []string{"ticker", "heartbeat"}
	productIds := strings.Split(pids, ",")

	u := url.URL{Scheme: "wss", Host: "ws-feed.exchange.coinbase.com"}
	log.Printf("connecting to %s\n", u.String())

	wsClient, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}

	subReq := &SubscribeRequest{
		RequestType: "subscribe",
		ProductIds:  productIds,
		Channels:    channels,
	}

	log.Printf("coinbase connector subscribing to ticker data for %s", aurora.BrightBlue(pids))
	err = wsClient.WriteJSON(subReq)
	if err != nil {
		return fmt.Errorf("error subscribing to %s for channels %s: %w", pids, channels, err)
	}

	go func() {
		for {
			_, message, err := wsClient.ReadMessage()
			if err != nil {
				return
			}
			c.sendData(message)
		}
	}()

	return nil
}

func (c *CoinbaseConnector) Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error {
	c.readHandlers = append(c.readHandlers, &handler)
	return nil
}

func (c *CoinbaseConnector) sendData(data []byte) {
	if len(c.readHandlers) == 0 {
		// Nothing to read
		return
	}

	var headers MessageHeaders
	err := json.Unmarshal(data, &headers)
	if err != nil {
		log.Printf("invalid coinbase message received '%s': %s", string(data), err.Error())
		return
	}

	if headers.MessageType == "subscriptions" {
		var subscriptions Subscriptions
		err := json.Unmarshal(data, &subscriptions)
		if err != nil {
			log.Printf("coinbase connector error reading subscriptions: %s", err.Error())
			return
		}
		for _, subscription := range subscriptions.Channels {
			log.Printf("coinbase connector subscribed to %s for %s", aurora.BrightBlue(subscription.Name), aurora.BrightBlue(strings.Join(subscription.ProductIDS, ",")))
		}
		return
	}

	if headers.MessageType == "heartbeat" {
		var heartbeat Heartbeat
		err := json.Unmarshal(data, &heartbeat)
		if err != nil {
			log.Printf("coinbase connector error reading heartbeat: %s", err.Error())
			return
		}
		log.Printf("coinbase connector received %s for %s with sequence id %d", aurora.BrightBlue(heartbeat.Type), aurora.BrightBlue(heartbeat.ProductID), heartbeat.Sequence)
		return
	}

	metadata := map[string]string{}

	errGroup, _ := errgroup.WithContext(context.Background())

	if len(c.readHandlers) == 0 {
		return
	}

	for _, handler := range c.readHandlers {
		readHandler := *handler
		errGroup.Go(func() error {
			_, err := readHandler(data, metadata)
			return err
		})
	}

	err = errGroup.Wait()
	if err != nil {
		log.Println(err.Error())
	}
}
