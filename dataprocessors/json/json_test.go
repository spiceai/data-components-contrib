package json

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/assert"
)

var (
	snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataprocessors/json"))
)

func TestJson(t *testing.T) {
	puppies, err := os.ReadFile("../../test/assets/data/json/puppies_valid.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	puppy, err := os.ReadFile("../../test/assets/data/json/puppy_valid.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	tweets, err := os.ReadFile("../../test/assets/data/json/tweets_valid.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Run("Init()", testInitFunc())
	t.Run("GetRecord() - array", testGetRecordFunc(puppies))
	t.Run("GetRecord() - single object", testGetRecordFunc(puppy))
	t.Run("GetRecord() -- with selected identifiers", testGetRecordSelectedIdentifiersFunc(puppies))
	t.Run("GetRecord() -- with selected measurements", testGetRecordSelectedMeasurementsFunc(puppies))
	t.Run("GetRecord() -- with a string value for some data points", testGetRecordSomeDataPointsFunc(tweets))
	t.Run("GetRecord() -- with an invalid string value for some data points", testGetRecordInvalidStringFunc(tweets))
	t.Run("GetRecord() called before Init()", testGetRecordNoInitFunc())
	t.Run("GetRecord() called twice", testGetRecordTwiceFunc(puppies))
	t.Run("GetRecord() updated with same data", testGetRecordSameDataFunc(puppies))
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewJsonProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params, nil, nil, nil, nil)
		assert.NoError(t, err)
	}
}

// Tests "GetRecord()"
func testGetRecordFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"ave_weight": "ave_weight",
			"population": "population",
		}

		categories := map[string]string{
			"city": "city",
		}

		tags := []string{
			"t1",
			"tags",
			"_tags",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, nil, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		if err != nil {
			t.Error(err)
			return
		}

		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetRecord()"
func testGetRecordSomeDataPointsFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"retweet_count": "retweet_count",
			"reply_count":   "reply_count",
		}

		categories := map[string]string{
			"lang": "lang",
		}

		tags := []string{
			"tags",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, nil, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		if err != nil {
			t.Error(err)
			return
		}

		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetRecord()" with selected identifiers
func testGetRecordSelectedIdentifiersFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		identifiers := map[string]string{
			"population_id": "population",
		}

		measurements := map[string]string{
			"population": "population",
		}

		categories := map[string]string{
			"city": "city",
		}

		tags := []string{
			"tags",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, identifiers, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		if err != nil {
			t.Error(err)
			return
		}

		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetRecord()" with selected measurements
func testGetRecordSelectedMeasurementsFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"population": "population",
		}

		categories := map[string]string{
			"city": "city",
		}

		tags := []string{
			"tags",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, nil, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		if err != nil {
			t.Error(err)
			return
		}

		snapshotter.SnapshotT(t, actualRecord)
	}
}

// Tests "GetRecord()" when given an invalid string data point
func testGetRecordInvalidStringFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"retweet_count": "retweet_count",
			"reply_count":   "reply_count",
		}

		categories := map[string]string{
			"favorited": "favorited",
		}

		tags := []string{
			"tags",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, nil, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		_, err = dp.GetRecord()
		assert.NotNil(t, err)
		if err != nil {
			assert.Error(t, err)
			assert.Equal(t, "error unmarshaling item 0: value is not a valid string or number", err.Error())
		}
	}
}

// Tests "GetRecord()" before Init() is called
func testGetRecordNoInitFunc() func(*testing.T) {
	return func(t *testing.T) {
		dp := NewJsonProcessor()

		obs, err := dp.GetRecord()
		assert.NoError(t, err)
		assert.Nil(t, obs)
	}
}

// Tests "GetRecord()" called twice
func testGetRecordTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"ave_weight": "ave_weight",
			"population": "population",
		}

		categories := map[string]string{
			"city": "city",
		}

		tags := []string{
			"tags",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, nil, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		assert.NoError(t, err)

		snapshotter.SnapshotT(t, actualRecord)

		actualRecord2, err := dp.GetRecord()
		assert.NoError(t, err)
		assert.Nil(t, actualRecord2)
	}
}

// Tests "GetRecord()" updated with same data
func testGetRecordSameDataFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"ave_weight": "ave_weight",
			"population": "population",
		}

		categories := map[string]string{
			"city": "city",
		}

		tags := []string{
			"tags",
		}

		dp := NewJsonProcessor()
		err := dp.Init(nil, nil, measurements, categories, tags)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		assert.NoError(t, err)

		snapshotter.SnapshotT(t, actualRecord)

		reader := bytes.NewReader(data)
		buffer := new(bytes.Buffer)
		_, err = io.Copy(buffer, reader)
		if err != nil {
			t.Error(err)
		}

		_, err = dp.OnData(buffer.Bytes())
		assert.NoError(t, err)

		actualRecord2, err := dp.GetRecord()
		assert.NoError(t, err)
		assert.Nil(t, actualRecord2)
	}
}

func TestUnmarshalString(t *testing.T) {
	t.Run("string value", testUnmarshalStringFunc("test-string", "test-string"))
	t.Run("int value", testUnmarshalStringFunc(int(123), "123"))
	t.Run("int64 value", testUnmarshalStringFunc(int64(123), "123"))
	t.Run("float64 value", testUnmarshalStringFunc(float64(123.123), "123.123"))
}

func testUnmarshalStringFunc(jsonVal interface{}, expected string) func(*testing.T) {
	return func(t *testing.T) {
		data, err := json.Marshal(jsonVal)
		if err != nil {
			t.Fatal(err)
		}

		var val json.RawMessage
		err = json.Unmarshal(data, &val)
		if err != nil {
			t.Fatal(err)
		}

		actual, err := unmarshalString(val)
		assert.NoError(t, err)

		assert.Equal(t, expected, actual)
	}
}
