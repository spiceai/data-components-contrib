package influxdb

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
	"github.com/influxdata/influxdb-client-go/domain"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataconnectors/influxdb"))

type mockClient struct {
	queryAPIFunc func(org string) api.QueryAPI
}

func (c *mockClient) Setup(ctx context.Context, username, password, org, bucket string, retentionPeriodHours int) (*domain.OnboardingResponse, error) {
	return nil, nil
}
func (c *mockClient) Ready(ctx context.Context) (bool, error)                  { return true, nil }
func (c *mockClient) Health(ctx context.Context) (*domain.HealthCheck, error)  { return nil, nil }
func (c *mockClient) Close()                                                   {}
func (c *mockClient) Options() *influxdb2.Options                              { return nil }
func (c *mockClient) ServerURL() string                                        { return "" }
func (c *mockClient) ServerUrl() string                                        { return "" }
func (c *mockClient) WriteAPI(org, bucket string) api.WriteAPI                 { return nil }
func (c *mockClient) WriteApi(org, bucket string) api.WriteApi                 { return nil }
func (c *mockClient) WriteAPIBlocking(org, bucket string) api.WriteAPIBlocking { return nil }
func (c *mockClient) WriteApiBlocking(org, bucket string) api.WriteApiBlocking { return nil }
func (c *mockClient) QueryAPI(org string) api.QueryAPI {
	return c.queryAPIFunc(org)
}
func (c *mockClient) QueryApi(org string) api.QueryApi         { return nil }
func (c *mockClient) AuthorizationsAPI() api.AuthorizationsAPI { return nil }
func (c *mockClient) AuthorizationsApi() api.AuthorizationsApi { return nil }
func (c *mockClient) OrganizationsAPI() api.OrganizationsAPI   { return nil }
func (c *mockClient) OrganizationsApi() api.OrganizationsApi   { return nil }
func (c *mockClient) UsersAPI() api.UsersAPI                   { return nil }
func (c *mockClient) UsersApi() api.UsersApi                   { return nil }
func (c *mockClient) DeleteAPI() api.DeleteAPI                 { return nil }
func (c *mockClient) DeleteApi() api.DeleteApi                 { return nil }
func (c *mockClient) BucketsAPI() api.BucketsAPI               { return nil }
func (c *mockClient) BucketsApi() api.BucketsApi               { return nil }
func (c *mockClient) LabelsAPI() api.LabelsAPI                 { return nil }
func (c *mockClient) LabelsApi() api.LabelsApi                 { return nil }

type mockQueryAPI struct {
	queryRawFunc func(ctx context.Context, query string, dialect *domain.Dialect) (string, error)
}

func (q *mockQueryAPI) QueryRaw(ctx context.Context, query string, dialect *domain.Dialect) (string, error) {
	return q.queryRawFunc(ctx, query, dialect)
}

func (q *mockQueryAPI) setQueryRaw(queryRawFunc func(ctx context.Context, query string, dialect *domain.Dialect) (string, error)) {
	q.queryRawFunc = queryRawFunc
}

func (q *mockQueryAPI) Query(ctx context.Context, query string) (*api.QueryTableResult, error) {
	return nil, nil
}

func TestInfluxDbConnector(t *testing.T) {
	params := map[string]string{
		"url":   "fake-url-for-test",
		"token": "fake-token-for-test",
	}

	t.Run("Init()", testInitFunc(params))
	t.Run("Read()", testReadFunc(params))
	t.Run("Read() with refresh", testReadWithRefreshFunc(params))
}

func TestInfluxDbConnectorQueries(t *testing.T) {
	t.Cleanup(func() {
		now = time.Now
	})

	defaultEpoch := time.Unix(1625439896, 0)

	setEpochExpectedQueries := []string{
		`from(bucket:"") |>
		range(start: 2021-07-04T23:04:56Z, stop: 2021-07-07T23:04:56Z) |>
		filter(fn: (r) => r["_measurement"] == "_measurement") |>
		filter(fn: (r) => r["_field"] == "_value") |>
		aggregateWindow(every: 2h0m0s, fn: mean, createEmpty: false)`,
	}

	t.Run("Read() set epoch", testQueriesFunc(defaultEpoch, 3*24*time.Hour, 2*time.Hour, setEpochExpectedQueries))

	now = clockwork.NewFakeClockAt(time.Unix(1633421096, 0)).Now
	zeroEpochExpectedQueries := []string{
		`from(bucket:"") |>
		range(start: 2021-09-28T08:04:56Z, stop: 2021-10-05T08:04:56Z) |>
		filter(fn: (r) => r["_measurement"] == "_measurement") |>
		filter(fn: (r) => r["_field"] == "_value") |>
		aggregateWindow(every: 1h0m0s, fn: mean, createEmpty: false)`,
		`from(bucket:"") |>
		range(start: 2021-10-05T07:04:56Z, stop: 2021-10-12T08:04:56Z) |>
		filter(fn: (r) => r["_measurement"] == "_measurement") |>
		filter(fn: (r) => r["_field"] == "_value") |>
		aggregateWindow(every: 1h0m0s, fn: mean, createEmpty: false)`,
	}

	t.Run("Read() zero epoch", testQueriesFunc(time.Time{}, 7*24*time.Hour, time.Hour, zeroEpochExpectedQueries))
}

func testInitFunc(params map[string]string) func(*testing.T) {
	c := NewInfluxDbConnector()

	return func(t *testing.T) {
		var epoch time.Time
		var period time.Duration
		var interval time.Duration

		err := c.Init(epoch, period, interval, params)
		assert.NoError(t, err)
	}
}

func testReadFunc(params map[string]string) func(*testing.T) {
	c := NewInfluxDbConnector()

	mockQueryAPI := mockQueryAPI{}
	mockClient := &mockClient{
		queryAPIFunc: func(org string) api.QueryAPI {
			return &mockQueryAPI
		},
	}
	c.SetInfluxdbClient(mockClient)

	return func(t *testing.T) {
		var epoch time.Time
		period := 7 * 24 * time.Hour
		interval := time.Hour

		expectedResult := "query-result"

		mockQueryAPI.setQueryRaw(func(ctx context.Context, query string, dialect *domain.Dialect) (string, error) {
			return expectedResult, nil
		})

		done := make(chan bool, 1)

		err := c.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
			done <- true
			return nil, nil
		})
		assert.NoError(t, err)

		params["refresh_interval"] = "0"
		err = c.Init(epoch, period, interval, params)
		if assert.NoError(t, err) {
			<-done
		}
	}
}

func testQueriesFunc(epoch time.Time, period time.Duration, interval time.Duration, expectedQueries []string) func(*testing.T) {
	params := map[string]string{
		"url":              "fake-url-for-test",
		"token":            "fake-token-for-test",
		"refresh_interval": "250ms",
	}

	c := NewInfluxDbConnector()

	mockQueryAPI := mockQueryAPI{}
	mockClient := &mockClient{
		queryAPIFunc: func(org string) api.QueryAPI {
			return &mockQueryAPI
		},
	}
	c.SetInfluxdbClient(mockClient)

	return func(t *testing.T) {
		expectedResult := "query-result"

		isDone := false
		numCalls := 0
		mockQueryAPI.setQueryRaw(func(ctx context.Context, query string, dialect *domain.Dialect) (string, error) {
			if isDone {
				return "", nil
			}
			assertEqualQuery(t, expectedQueries[numCalls], query)
			numCalls++
			return expectedResult, nil
		})

		wg := sync.WaitGroup{}
		wg.Add(len(expectedQueries))

		err := c.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
			if isDone {
				return nil, nil
			}
			assert.Equal(t, expectedResult, string(data))
			wg.Done()
			return nil, nil
		})
		assert.NoError(t, err)

		err = c.Init(epoch, period, interval, params)
		if assert.NoError(t, err) {
			wg.Wait()
			isDone = true
		}
	}
}

func testReadWithRefreshFunc(params map[string]string) func(*testing.T) {
	c := NewInfluxDbConnector()

	mockQueryAPI := mockQueryAPI{}
	mockClient := &mockClient{
		queryAPIFunc: func(org string) api.QueryAPI {
			return &mockQueryAPI
		},
	}
	c.SetInfluxdbClient(mockClient)

	return func(t *testing.T) {
		var epoch time.Time
		period := 7 * 24 * time.Hour
		interval := time.Hour

		expectedResult := "query-result"

		mockQueryAPI.setQueryRaw(func(ctx context.Context, query string, dialect *domain.Dialect) (string, error) {
			return expectedResult, nil
		})

		readCount := 0
		err := c.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
			assert.Equal(t, expectedResult, string(data))
			readCount++
			return nil, nil
		})
		assert.NoError(t, err)

		params["refresh_interval"] = "100ms"
		err = c.Init(epoch, period, interval, params)
		if assert.NoError(t, err) {
			timer := time.NewTimer(time.Second)
			<-timer.C

			assert.Equal(t, 10, readCount)
		}
	}
}

func assertEqualQuery(t assert.TestingT, expectedQuery string, query string) bool {
	return assert.Equal(t, cleanQuery(expectedQuery), cleanQuery(query))
}

func cleanQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	return strings.ReplaceAll(trimmed, "\t", "")
}
