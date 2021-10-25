package twitter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/logrusorgru/aurora"
	"golang.org/x/sync/errgroup"
)

const (
	TwitterConnectorName string = "twitter"
)

type TwitterConnector struct {
	client       *twitter.Client
	readHandlers []*func(data []byte, metadata map[string]string) ([]byte, error)
}

func NewTwitterConnector() *TwitterConnector {
	return &TwitterConnector{}
}

func (c *TwitterConnector) Init(epoch time.Time, period time.Duration, interval time.Duration, params map[string]string) error {
	ck := params["consumer_key"]
	if ck == "" {
		return errors.New("consumer_key is required")
	}

	cs := params["consumer_secret"]
	if cs == "" {
		return errors.New("consumer_secret is required")
	}

	at := params["access_token"]
	if at == "" {
		return errors.New("access_token is required")
	}

	as := params["access_secret"]
	if as == "" {
		return errors.New("access_secret is required")
	}

	filter := params["filter"]
	if filter == "" {
		return errors.New("filter is required")
	}

	config := oauth1.NewConfig(ck, cs)
	token := oauth1.NewToken(at, as)
	httpClient := config.Client(oauth1.NoContext, token)

	// Twitter client
	c.client = twitter.NewClient(httpClient)

	verifyParams := &twitter.AccountVerifyParams{}
	user, _, err := c.client.Accounts.VerifyCredentials(verifyParams)
	if err != nil {
		return fmt.Errorf("failed to verify credentials: %s", err.Error())
	}

	log.Println(fmt.Sprintf("twitter data connector: verified credentials for %s", aurora.BrightBlue(user.ScreenName)))

	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		c.sendData(tweet)
	}

	filterParams := &twitter.StreamFilterParams{
		Track:         []string{filter},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := c.client.Streams.Filter(filterParams)
	if err != nil {
		return fmt.Errorf("failed to start stream with filter %s: %s", aurora.BrightBlue(filter), err.Error())
	}

	log.Println(aurora.Green(fmt.Sprintf("started reading twitter stream with filter %s", aurora.BrightBlue(filter))))

	go demux.HandleChan(stream.Messages)

	return nil
}

func (c *TwitterConnector) Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error {
	c.readHandlers = append(c.readHandlers, &handler)
	return nil
}

func (c *TwitterConnector) sendData(tweets ...*twitter.Tweet) {
	if len(c.readHandlers) == 0 {
		// Nothing to read
		return
	}

	metadata := map[string]string{}
	metadata["type"] = "tweet"

	errGroup, _ := errgroup.WithContext(context.Background())

	if len(c.readHandlers) == 0 {
		return
	}

	data, err := json.Marshal(tweets)
	if err != nil {
		log.Println(err.Error())
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
