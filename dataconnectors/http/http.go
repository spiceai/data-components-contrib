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

func (con *HttpConnector) Init(epoch time.Time, period time.Duration, interval time.Duration, params map[string]string) error {
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

	con.client = &http.Client{
		Timeout: timeout,
	}

	con.request = &http.Request{
		Method: method,
		URL:    url,
	}

	if pollingInterval <= 0 {
		err = con.doRequest()
		if err != nil {
			log.Printf("Http connector request error: %s", err)
		}
		return nil
	}

	con.requestTicker = time.NewTicker(pollingInterval)
	con.stop = make(chan bool)

	go func() {
		err = con.doRequest()
		if err != nil {
			log.Printf("Http connector %s: %s", url, aurora.BrightRed(err))
		}
		for {
			select {
			case <-con.stop:
				return
			case <-con.requestTicker.C:
				err := con.doRequest()
				if err != nil {
					log.Printf("Http connector %s: %s\n", url, aurora.BrightRed(err))
				}
			}
		}
	}()

	return nil
}

func (con *HttpConnector) Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error {
	con.readHandlers = append(con.readHandlers, &handler)
	return nil
}

func (con *HttpConnector) doRequest() error {
	startTime := time.Now()
	response, err := con.client.Do(con.request)
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

	for _, handler := range con.readHandlers {
		_, err := (*handler)(body, metadata)
		if err != nil {
			return fmt.Errorf("failed to process response: %w", err)
		}
	}

	return nil
}
