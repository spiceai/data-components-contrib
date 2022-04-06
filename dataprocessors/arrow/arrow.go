package arrow

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	apache_arrow "github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/flight"
)

const (
	ArrowProcessorName string = "arrow"
)

type ArrowProcessor struct {
	data []byte
}

func NewArrowProcessor() *ArrowProcessor {
	return &ArrowProcessor{}
}

func (p *ArrowProcessor) Init(params map[string]string, identifiers map[string]string, measurements map[string]string, categories map[string]string, tags []string) error {
	return nil
}

func (p *ArrowProcessor) OnData(data []byte) ([]byte, error) {
	p.data = data
	return data, nil
}

func (p *ArrowProcessor) GetRecord() (apache_arrow.Record, error) {
	if p.data == nil {
		return nil, nil
	}

	var out uint64
	buf := bytes.NewReader(p.data)
	err := binary.Read(buf, binary.BigEndian, &out)
	if err != nil {
		return nil, fmt.Errorf("binary.Read failed: %w", err)
	}

	stream := (*flight.FlightService_DoGetClient)(unsafe.Pointer(uintptr(out)))

	reader, err := flight.NewRecordReader(*stream)
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	if reader.Next() {
		record := reader.Record()
		return record, nil
	}

	return nil, fmt.Errorf("no record could be read")
}
