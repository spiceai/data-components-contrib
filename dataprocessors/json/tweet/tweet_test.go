package tweet_test

import (
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataprocessors/json/tweet"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../../test/assets/snapshots/dataprocessors/json/tweet"))

func TestTweetJson(t *testing.T) {
	data, err := os.ReadFile("../../../test/assets/data/json/tweet_valid.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	tweetJsonFormat := &tweet.TweetJsonFormat{}

	schema := tweetJsonFormat.GetSchema()
	err = snapshotter.SnapshotMulti("schema.json", schema)
	if err != nil {
		t.Fatal(err.Error())
	}

	observations, err := tweetJsonFormat.GetObservations(data)
	if assert.NoError(t, err) {
		err = snapshotter.SnapshotMulti("observations.json", observations)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
}
