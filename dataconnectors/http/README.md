# HTTP Data Connector

The HTTP data connector will fetch data from a HTTP/HTTPS endpoint.

It will currently fetch on init or on a polling interval as specified by the `polling_interval` parameter.

The result can be processed with a [data processor](../../dataprocessors/README.md).

## Supported parameters

- `url` [Required] The URL to fetch.
- `method` [Optional] The HTTP method to use. Defaults to `GET`.
- `timeout` [Optional] The request timeout to use. Defaults to `10s`.
- `polling_interval` [Optional] If provided, the connector will poll the endpoint on this interval.

## Example Dataspace

```yaml
dataspaces:
  - from: spiceai
    name: gasfees
    fields:
      - name: slow
      - name: normal
      - name: fast
      - name: instant
    data:
      connector:
        name: http
        params:
          url: https://data.spiceai.io/eth/v1/gasfees
      processor:
        name: json
```
