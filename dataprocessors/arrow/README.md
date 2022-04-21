# Arrow Processor

To use the Arrow Processor use this data config:

```yaml
data:
  processor:
    name: arrow
```

## Params

| Name          | Supported Values       | Description                         |
| ------------- | ---------------------- | ----------------------------------- |
| time_selector | A top-level field name | Specifies the field to use for time |

## Example Dataspace Config

Paired with the [Apache Arrow Flight Data Connector](../../dataconnectors/flight/README.md).

```yaml
dataspaces:
  - from: spice
    name: sql
    identifiers:
      - name: number
    measurements:
      - name: gas_used
    data:
      connector:
        name: flight
        params:
          password: <api_key>
          sql: SELECT number, timestamp, gas_used FROM eth.recent_blocks ORDER BY number DESC
      processor:
        name: arrow
        params:
          time_selector: timestamp
```
