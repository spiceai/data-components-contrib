package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/observations"
	spice_time "github.com/spiceai/spiceai/pkg/time"
	"github.com/spiceai/spiceai/pkg/util"
)

const (
	JsonProcessorName string = "json"
)

type JsonProcessor struct {
	timeFormat   string
	measurements map[string]string
	categories   map[string]string

	dataMutex    sync.RWMutex
	data         [][]byte
	lastDataHash []byte
}

func NewJsonProcessor() *JsonProcessor {
	return &JsonProcessor{}
}

func (p *JsonProcessor) Init(params map[string]string, measurements map[string]string, categories map[string]string) error {
	if val, ok := params["time_format"]; ok {
		p.timeFormat = val
	}
	p.measurements = measurements
	p.categories = categories

	return nil
}

func (p *JsonProcessor) OnData(data []byte) ([]byte, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	var currentData []byte
	if p.data != nil && len(p.data) > 0 {
		currentData = p.data[len(p.data)-1]
	}

	newDataHash, err := util.ComputeNewHash(currentData, p.lastDataHash, data)
	if err != nil {
		return nil, fmt.Errorf("error computing new data hash in json processor: %w", err)
	}

	if newDataHash != nil {
		// Only update data if new
		p.data = append(p.data, data)
		p.lastDataHash = newDataHash
	}

	return data, nil
}

func (p *JsonProcessor) GetObservations() ([]observations.Observation, error) {
	if p.data == nil {
		return nil, nil
	}

	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()

	if p.data == nil {
		return nil, nil
	}

	var newObservations []observations.Observation

	for _, data := range p.data {
		firstChar := string(data[:1])
		if firstChar == "{" {
			var item map[string]json.RawMessage

			err := json.Unmarshal(data, &item)
			if err != nil {
				return nil, err
			}
			o, err := p.newObservationFromJson(0, item)
			if err != nil {
				return nil, fmt.Errorf("error unmarshaling item: %s", err.Error())
			}
			newObservations = append(newObservations, *o)
		} else {
			var items []map[string]json.RawMessage

			err := json.Unmarshal(data, &items)
			if err != nil {
				return nil, err
			}

			for index, item := range items {
				o, err := p.newObservationFromJson(index, item)
				if err != nil {
					return nil, fmt.Errorf("error unmarshaling item %d: %s", index, err.Error())
				}
				newObservations = append(newObservations, *o)
			}
		}
	}

	p.data = nil

	return newObservations, nil
}

func (p *JsonProcessor) newObservationFromJson(index int, item map[string]json.RawMessage) (*observations.Observation, error) {
	t, err := unmarshalTime(p.timeFormat, item["time"])
	if err != nil {
		return nil, err
	}

	measurements := make(map[string]float64)

	for fieldName, selector := range p.measurements {
		if val, ok := item[selector]; ok {
			var m float64
			err = json.Unmarshal(val, &m)
			if err != nil {
				var str string
				strErr := json.Unmarshal(val, &str)
				if strErr != nil {
					return nil, err
				}
				m, err = strconv.ParseFloat(str, 64)
				if err != nil {
					return nil, err
				}
			}
			measurements[fieldName] = m
		}
	}

	categories := make(map[string]string)

	for fieldName, selector := range p.categories {
		if val, ok := item[selector]; ok {
			var str string
			err = json.Unmarshal(val, &str)
			if err != nil {
				return nil, err
			}
			categories[fieldName] = str
		}
	}

	observation := &observations.Observation{
		Time: t.Unix(),
	}

	if val, ok := item["tags"]; ok {
		var tags string
		err = json.Unmarshal(val, &tags)
		if err != nil {
			return nil, err
		}
		observation.Tags = strings.Split(tags, " ")
	}

	if len(measurements) > 0 {
		observation.Measurements = measurements
	}

	if len(categories) > 0 {
		observation.Categories = categories
	}

	return observation, nil
}

func unmarshalTime(timeFormat string, data []byte) (*time.Time, error) {
	st := spice_time.Time{}
	err := st.UnmarshalJSON(data)
	if err != nil {
		return nil, fmt.Errorf("time format is invalid: %s", string(data))
	}

	if st.Integer != nil {
		t := time.Unix(*st.Integer, 0)
		return &t, nil
	}

	if st.String != nil {
		var t time.Time
		t, err = spice_time.ParseTime(*st.String, timeFormat)
		if err != nil {
			return nil, fmt.Errorf("time format is invalid: %s", *st.String)
		}
		return &t, nil
	}

	return nil, errors.New("did not include a time component")
}
