# Apache Arrow Flight Data Connector

The Flight data connector will query and fetch data from an [Apache Flight](https://arrow.apache.org/docs/format/Flight.html) endpoint.

## Supported parameters

- `sql` [Required] File or string containing a SQL query to execute.
- `username` [Optional] Username for authentication (omit with password if no auth).
- `password` [Optional] Password for authentication (omit with username if no auth).

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
          password: <api_key>
          sql: query.sql
      processor:
        name: arrow
```
