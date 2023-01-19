package flight

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	FlightConnectorName string = "flight"
)

type FlightConnector struct {
	client   flight.Client
	username string
	password string
	query    []byte

	stream *flight.FlightService_DoGetClient
}

func NewFlightConnector() *FlightConnector {
	return &FlightConnector{}
}

func (c *FlightConnector) Init(epoch time.Time, period time.Duration, interval time.Duration, params map[string]string) error {
	sqlPath := strings.TrimSpace(params["sql"])
	c.username = strings.TrimSpace(params["username"])
	c.password = strings.TrimSpace(params["password"])

	url := params["url"]
	if url == "" {
		return fmt.Errorf("No url specified")
	}

	client, err := flight.NewClientWithMiddleware(url, nil, nil, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	if err != nil {
		return fmt.Errorf("failed to create flight client: %w", err)
	}

	c.client = client

	if _, err = os.Stat(sqlPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.query = []byte(sqlPath)
		} else {
			return fmt.Errorf("failed to open sql file %s: %w", sqlPath, err)
		}
	}

	if c.query == nil {
		sqlContent, err := os.ReadFile(sqlPath)
		if err != nil {
			return fmt.Errorf("failed to open file '%s': %w", sqlPath, err)
		}
		c.query = sqlContent
	}

	return nil
}

func (c *FlightConnector) Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error {
	if c.client == nil {
		return fmt.Errorf("No flight client: init was forgotten or got an error")
	}

	clientContext := metadata.NewOutgoingContext(context.TODO(),
		metadata.Pairs("content-type", "application/grpc+proto"))
	if c.username != "" || c.password != "" {
		newContext, err := c.client.AuthenticateBasicToken(clientContext, c.username, c.password)
		if err != nil {
			return fmt.Errorf("failed to authenticate flight client: %w", err)
		}
		clientContext = newContext
	}

	desc := &flight.FlightDescriptor{
		Type: flight.FlightDescriptorCMD,
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
