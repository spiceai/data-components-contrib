package json

import (
	"bytes"
	"encoding/json"
	"errors"
)

type Time struct {
	Integer *int64
	String  *string
}

type ObservationPoint struct {
	Time *Time              `json:"time"`
	Data map[string]float64 `json:"data"`
	Tags []string           `json:"tags,omitempty"`
}

func UnmarshalObservationPoint(data []byte) (ObservationPoint, error) {
	var r ObservationPoint
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *ObservationPoint) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (x *Time) UnmarshalJSON(data []byte) error {
	err := unmarshalTime(data, &x.Integer, &x.String)
	if err != nil {
		return err
	}
	return nil
}

func (x *Time) MarshalJSON() ([]byte, error) {
	return marshalTime(x.Integer, x.String)
}

func unmarshalTime(data []byte, pi **int64, ps **string) error {
	*pi = nil
	*ps = nil

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return err
	}

	switch v := tok.(type) {
	case json.Number:
		i, err := v.Int64()
		if err == nil {
			*pi = &i
			return nil
		}
		return err
	case string:
		*ps = &v
		return nil
	}
	return errors.New("cannot unmarshal Time")

}

func marshalTime(pi *int64, ps *string) ([]byte, error) {
	if pi != nil {
		return json.Marshal(*pi)
	}
	if ps != nil {
		return json.Marshal(*ps)
	}
	return nil, errors.New("time must not be null")
}
