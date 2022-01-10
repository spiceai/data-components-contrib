# Spice.ai Data Connectors

Currently supported connectors:

- [File](file/file.go)
- [HTTP](http/http.go)
- [InfluxDB](influxdb/influxdb.go)
- [Coinbase](coinbase/README.md)
- [Twitter](twitter/twitter.go)

## Contribution guide

Writing a data connector means implementing the `DataConnector` interface defined at [dataconnector.go](dataconnector.go) and adding it to the `NewDataConnector` factory function.

```golang
type DataConnector interface {
    Init(Epoch time.Time, Period time.Duration, Interval time.Duration, params map[string]string) error
    Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error
}
```

Data Connectors are consumed in the [Spice.ai pod](https://docs.spiceai.org/concepts/#pod) manifest in the `data` section. E.g.

```yaml
data:
  connector:
  name: file
  params:
    path: my-data.csv
```

The data connector name is self-declared by the component, but must be unique across all components.
