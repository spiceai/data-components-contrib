package dataconnectors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDataConnector(t *testing.T) {
	t.Run("NewDataConnector() - Invalid connector", testNewDataConnectorUnknownFunc())
}

func testNewDataConnectorUnknownFunc() func(*testing.T) {
	return func(t *testing.T) {
		_, err := NewDataConnector("does-not-exist")
		assert.Error(t, err)
	}
}

func TestCoinbaseFactory(t *testing.T) {
	c, err := NewDataConnector("coinbase")
	assert.NoError(t, err)
	assert.NotNil(t, c)
}
