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

	log.Printf("subscribing to coinbase ticker data for '%s'", pids)
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
		log.Printf("invalid coinbase message received '%s': %s\n", string(data), err.Error())
		return
	}

	if headers.MessageType == "subscription" {
		log.Printf("coinbase connector subscribed to: %s\n", string(data))
		return
	}

	if headers.MessageType == "heartbeat" {
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
