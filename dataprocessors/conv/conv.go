package conv

import (
	"strconv"
	"strings"
)

func ParseMeasurement(measurement string) (float64, error) {
	if strings.HasPrefix(measurement, "0x") {
		val, err := strconv.ParseUint(measurement[2:], 16, 64)
		if err != nil {
			return 0, err
		}
		return float64(val), nil
	}

	return strconv.ParseFloat(measurement, 64)
}
