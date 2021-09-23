package influxdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/domain"
	"golang.org/x/sync/errgroup"
)

const (
	InfluxDbConnectorName string = "influxdb"
)

type InfluxDbConnector struct {
	client             influxdb2.Client
	readHandlers       []*func(data []byte, metadata map[string]string) ([]byte, error)

	lastFetchPeriodEnd time.Time
	lastError		   error

	dataMutex sync.RWMutex
	data      []byte

	org                string
	bucket             string
	field              string
	measurement        string
	refreshInterval    time.Duration
}

func NewInfluxDbConnector() *InfluxDbConnector {
	return &InfluxDbConnector{
		refreshInterval: 15 * time.Second,
		dataMutex:       sync.RWMutex{},
	}
}

func (c *InfluxDbConnector) Init(epoch time.Time, period time.Duration, interval time.Duration, params map[string]string) error {
	if _, ok := params["url"]; !ok {
		return errors.New("influxdb connector requires the 'url' parameter to be set")
	}

	if _, ok := params["token"]; !ok {
		return errors.New("influxdb connector requires the 'token' parameter to be set")
	}

	c.client = influxdb2.NewClient(params["url"], params["token"])

	if org, ok := params["org"]; ok {
		c.org = org
	}

	if bucket, ok := params["bucket"]; ok {
		c.bucket = bucket
	}

	if field, ok := params["field"]; ok {
		c.field = field
	} else {
		// Default to _value
		c.field = "_value"
	}

	if measurement, ok := params["measurement"]; ok {
		c.measurement = measurement
	} else {
		// Default to _measurement
		c.measurement = "_measurement"
	}

	if refreshInterval, ok := params["refresh_interval"]; ok {
		ri, err := time.ParseDuration(refreshInterval)
		if err != nil {
			return fmt.Errorf("invalid refresh_interval '%s': %s", refreshInterval, err)
		}
		if ri.Seconds() < 5 {
			return fmt.Errorf("invalid refresh_interval '%s': interval must be >= 5 seconds", refreshInterval)
		}
		c.refreshInterval = ri
	}

	ticker := time.NewTicker(c.refreshInterval)
    done := make(chan bool)
    go func() {
        for {
            select {
            case <-done:
                return
            case <-ticker.C:
                err := c.refreshData(epoch, period, interval)
				if err != nil && c.lastError != nil {
					// Two errors in a row, stop refresh
					log.Printf("InfluxDb connector refresh error: %s\n", c.lastError.Error())
					return
				}
				c.lastError = err
            }
        }
    }()

	return nil
}

func (c *InfluxDbConnector) Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error {
	c.readHandlers = append(c.readHandlers, &handler)
	return nil
}

func (c *InfluxDbConnector) refreshData(epoch time.Time, period time.Duration, interval time.Duration) error {
	c.dataMutex.Lock()
	defer c.dataMutex.Unlock()

	periodStart := epoch
	periodEnd := epoch.Add(period)

	if !c.lastFetchPeriodEnd.IsZero() && c.lastFetchPeriodEnd.After(epoch) {
		// If we've already fetched, only fetch the difference with an interval overlap
		periodStart = c.lastFetchPeriodEnd.Add(-interval)
	}

	if periodStart == periodEnd || periodStart.After(periodEnd) {
		// No new data to fetch
		return nil
	}

	periodStartStr := periodStart.Format(time.RFC3339)
	periodEndStr := periodEnd.Format(time.RFC3339)
	intervalStr := fmt.Sprintf("%ds", int64(interval.Seconds()))

	query := fmt.Sprintf(`
		from(bucket:"%s") |>
		range(start: %s, stop: %s) |>
		filter(fn: (r) => r["_measurement"] == "%s") |>
		filter(fn: (r) => r["_field"] == "%s") |>
		aggregateWindow(every: %s, fn: mean, createEmpty: false)
    `, c.bucket, periodStartStr, periodEndStr, c.measurement, c.field, intervalStr)

	header := true
	annotations := []domain.DialectAnnotations{"group", "datatype", "default"}
	dateTimeFormat := domain.DialectDateTimeFormatRFC3339
	dialect := &domain.Dialect{
		Header:         &header,
		Annotations:    &annotations,
		DateTimeFormat: &dateTimeFormat,
	}

	result, err := c.client.QueryAPI(c.org).QueryRaw(context.Background(), query, dialect)
	if err != nil {
		log.Printf("InfluxDb query failed: %v", err)
		return err
	}

	data := []byte(result)

	c.data = data
	c.lastFetchPeriodEnd = periodEnd

	err = c.sendData(periodStartStr, periodEndStr)
	if err != nil {
		return err
	}

	return nil
}

func (c *InfluxDbConnector) sendData(periodStart string, periodEnd string) error {
	if len(c.readHandlers) == 0 {
		// Nothing to read
		return nil
	}

	metadata := map[string]string{}
	metadata["start"] = periodStart
	metadata["end"] = periodEnd

	errGroup, _ := errgroup.WithContext(context.Background())

	for _, handler := range c.readHandlers {
		readHandler := *handler
		errGroup.Go(func() error {
			_, err := readHandler(c.data, metadata)
			return err
		})
	}

	return errGroup.Wait()
}