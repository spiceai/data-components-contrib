# Spice.ai Data Processors

Currently supported processors:

- [CSV](csv/csv.go)
- [Flux CSV](flux/fluxcsv.go)
- [JSON](json/README.md)

## Contribution guide

Writing a data processor means implementing the `DataProcessor` interface defined at [dataprocessor.go](dataprocessor.go) and adding it to the `NewDataProcessor` factory function.

```golang
type DataProcessor interface {
	Init(params map[string]string) error
	OnData(data []byte) ([]byte, error)
	GetRecord() (arrow.Record, error)
	GetState(fields *[]string) ([]*state.State, error)
}
```

Data Processors are consumed in the [Spice.ai pod](https://docs.spiceai.org/concepts/#pod) manifest in the `data` section. E.g.

```yaml
data:
  processor:
    name: flux-csv
```

The data processor name is self-declared by the component, but must be unique across all components.
