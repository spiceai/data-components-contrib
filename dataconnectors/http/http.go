package http

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/logrusorgru/aurora"
)

const (
	HttpConnectorName string = "http"
)

type HttpConnector struct {
	client       *http.Client
	request      *http.Request
	readHandlers []*func(data []byte, metadata map[string]string) ([]byte, error)

	requestTicker *time.Ticker
	stop          chan bool
}

func NewHttpConnector() *HttpConnector {
	return &HttpConnector{}
}

func (c *HttpConnector) Init(epoch time.Time, period time.Duration, interval time.Duration, params map[string]string) error {
	urlParam := params["url"]
	if urlParam == "" {
		return errors.New("url is required")
	}

	url, err := url.Parse(urlParam)
	if err != nil {
		return fmt.Errorf("invalid url: %s", err)
	}

	method := params["method"]
	if method == "" {
		method = "GET"
	}

	timeoutParam := params["timeout"]
	if timeoutParam == "" {
		timeoutParam = "5s"
	}
	timeout, err := time.ParseDuration(timeoutParam)
	if err != nil {
		return fmt.Errorf("invalid timeout: %s", err)
	}

	pollingIntervalParam := params["polling_interval"]
	var pollingInterval time.Duration
	if pollingIntervalParam != "" {
		pollingInterval, err = time.ParseDuration(pollingIntervalParam)
		if err != nil {
			return fmt.Errorf("invalid polling_interval: %s", err)
		}
	}

	c.client = &http.Client{
		Timeout: timeout,
	}

	c.request = &http.Request{
		Method: method,
		URL:    url,
	}

	if pollingInterval <= 0 {
		err = c.doRequest()
		if err != nil {
			log.Printf("Http connector request error: %s", err)
		}
		return nil
	}

	c.requestTicker = time.NewTicker(pollingInterval)
	c.stop = make(chan bool)

	go func() {
		err = c.doRequest()
		if err != nil {
			log.Printf("Http connector %s: %s", url, aurora.BrightRed(err))
		}
		for {
			select {
			case <-c.stop:
				return
			case <-c.requestTicker.C:
				err := c.doRequest()
				if err != nil {
					log.Printf("Http connector %s: %s\n", url, aurora.BrightRed(err))
				}
			}
		}
	}()

	return nil
}

func (c *HttpConnector) Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error {
	c.readHandlers = append(c.readHandlers, &handler)
	return nil
}

func (c *HttpConnector) doRequest() error {
	startTime := time.Now()
	response, err := c.client.Do(c.request)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed with status code %d", response.StatusCode)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	duration := time.Since(startTime)

	metadata := map[string]string{}
	metadata["time"] = startTime.Format(time.RFC3339Nano)
	metadata["status_code"] = fmt.Sprintf("%d", response.StatusCode)
	metadata["status"] = response.Status
	metadata["content_length"] = fmt.Sprintf("%d", len(body))
	metadata["content_type"] = response.Header.Get("Content-Type")
	metadata["content_encoding"] = response.Header.Get("Content-Encoding")
	metadata["duration_ms"] = fmt.Sprintf("%d", duration.Milliseconds())

	for _, handler := range c.readHandlers {
		_, err := (*handler)(body, metadata)
		if err != nil {
			return fmt.Errorf("failed to process response: %w", err)
		}
	}

	return nil
}
