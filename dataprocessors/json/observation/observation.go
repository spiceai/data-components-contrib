package observation

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/spiceai/spiceai/pkg/api/observation"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
)

type ObservationJsonFormat struct {
}

func (s *ObservationJsonFormat) GetSchema() []byte {
	return observation.JsonSchema()
}

func (s *ObservationJsonFormat) GetObservations(data []byte) ([]observations.Observation, error) {
	var observationPoints []observation.Observation = make([]observation.Observation, 0)

	err := json.Unmarshal(data, &observationPoints)
	if err != nil {
		return nil, err
	}

	var newObservations []observations.Observation
	for _, point := range observationPoints {
		var ts int64
		var err error

		if point.Time.Integer != nil {
			ts = *point.Time.Integer
		} else if point.Time.String != nil {
			var t time.Time
			t, err = time.Parse(time.RFC3339, *point.Time.String)
			if err != nil {
				// This should never happen as the schema validation would have caught this
				return nil, fmt.Errorf("observation time format is invalid: %s", *point.Time.String)
			}
			ts = t.Unix()
		} else {
			// This should never happen as the schema validation would have caught this
			return nil, fmt.Errorf("observation did not include a time component")
		}

		data := make(map[string]float64, len(point.Data))

		for key, val := range point.Data {
			if val.Float64 != nil {
				data[key] = *val.Float64
			} else {
				data[key], err = strconv.ParseFloat(*val.String, 64)
				if err != nil {
					return nil, err
				}
			}
		}

		observation := observations.Observation{
			Time: ts,
			Data: data,
			Tags: point.Tags,
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations, nil
}

func (s *ObservationJsonFormat) GetState(data []byte, validFields []string) ([]*state.State, error) {
	// TODO
	return nil, nil
}
