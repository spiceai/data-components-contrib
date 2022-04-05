# Flight Connector

The Flight data connector will fetch data from an [Apache Flight](https://arrow.apache.org/docs/format/Flight.html) endpoint.

## Supported parameters

- `sql` [Required] File containing SQL query to execute.
- `key` [Required] API key to use.

## Example Dataspace

```yaml
dataspaces:
  - from: spice
    name: sql
    fields:
      - name: number
      - name: timestamp
      - name: gas_used
    data:
      connector:
        name: flight
        params:
          api_key: 1234|abcd
          sql: query.sql 
      processor:
        name: arrow
```