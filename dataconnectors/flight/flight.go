package flight

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v7/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	FlightConnectorName string = "flight"
)

type FlightConnector struct {
	client flight.Client
	key    string
	query  []byte

	stream *flight.FlightService_DoGetClient
}

func NewFlightConnector() *FlightConnector {
	return &FlightConnector{}
}

func (c *FlightConnector) Init(epoch time.Time, period time.Duration, interval time.Duration, params map[string]string) error {
	sqlPath := params["sql"]
	apiKey := params["api_key"]
	c.key = apiKey

	url := params["url"]
	if url == "" {
		return fmt.Errorf("No url specified")
	}

	client, err := flight.NewFlightClient(url, nil, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	if err != nil {
		return fmt.Errorf("failed to create flight client: %w", err)
	}
	c.client = client

	sqlContent, err := ioutil.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to open file '%s': %w", sqlPath, err)
	}
	c.query = sqlContent

	return nil
}

func (c *FlightConnector) Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error {
	if c.client == nil {
		return fmt.Errorf("No flight client: init was forgotten or got an error")
	}
	clientContext, err := c.client.AuthenticateBasicToken(context.Background(), "", c.key)
	if err != nil {
		return fmt.Errorf("failed to authenticate flight client: %w", err)
	}

	desc := &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd:  c.query,
	}

	info, err := c.client.GetFlightInfo(clientContext, desc)
	if err != nil {
		return fmt.Errorf("failed to get flight info: %w", err)
	}

	stream, err := c.client.DoGet(clientContext, info.Endpoint[0].Ticket)
	if err != nil {
		return fmt.Errorf("failed to receive data stream: %s", err.Error())
	}
	c.stream = &stream

	metadata := map[string]string{}
	_, err = handler(*(*[]byte)(unsafe.Pointer(&stream)), metadata)
	return nil
}
