# Spice.ai Data Connectors

Currently supported connectors:

- [File](file/file.go)
- [InfluxDB](influxdb/influxdb.go)

> Data Connectors currently only support fetching data. Spice.ai will move to a streaming model in v0.2.0-alpha. See the [Spice.ai Roadmap](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md) for more details.

## Contribution guide

Writing a data connector means implementing the `DataConnector` interface defined at [dataconnector.go](dataconnector.go) and adding it to the `NewDataConnector` factory function.

```golang
type DataConnector interface {
	Init(params map[string]string) error
	FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]byte, error)
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
