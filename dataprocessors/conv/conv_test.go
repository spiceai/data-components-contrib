package conv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseMeasurement(t *testing.T) {
	t.Run("Test 0x0", func(t *testing.T) {
		val, err := ParseMeasurement("0x0")
		if assert.NoError(t, err) {
			assert.Equal(t, float64(0), val)
		}
	})

	t.Run("Test 0x1fdaae6df2", func(t *testing.T) {
		val, err := ParseMeasurement("0x1fdaae6df2")
		if assert.NoError(t, err) {
			assert.Equal(t, float64(1.36812850674e+11), val)
		}
	})

	t.Run("Test 0", func(t *testing.T) {
		val, err := ParseMeasurement("0")
		if assert.NoError(t, err) {
			assert.Equal(t, float64(0), val)
		}
	})

	t.Run("Test 0.0", func(t *testing.T) {
		val, err := ParseMeasurement("0.0")
		if assert.NoError(t, err) {
			assert.Equal(t, float64(0), val)
		}
	})

	t.Run("Test 123456", func(t *testing.T) {
		val, err := ParseMeasurement("123456")
		if assert.NoError(t, err) {
			assert.Equal(t, float64(123456), val)
		}
	})

	t.Run("Test 123456789.123456789", func(t *testing.T) {
		val, err := ParseMeasurement("123456789.123456789")
		if assert.NoError(t, err) {
			assert.Equal(t, float64(123456789.123456789), val)
		}
	})
}
