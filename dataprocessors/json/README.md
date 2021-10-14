# JSON Processor

To use the JSON Processor use this data config:

```yaml
data:
  processor:
    name: json
```

By default the JSON processor will consume the [Spice.ai observation json format](https://github.com/spiceai/spiceai/blob/trunk/pkg/api/observation/observation_schema.json).

An example payload with the observations schema looks like this:

```json
[
  {
    "data": {
      "eventId": 21052.59,
      "rating": 4,
      "speed": 26,
      "height": 81,
      "target": 12,
      <more fields here>
    },
    "tags": ["tagA", "tagB"],
    "time": 1631499271
  }
]
```

The values of the properties defined in data must be JSON numbers.

The value of time must either be a Unix timestamp or a string conforming to RFC3339 i.e. 1985-04-12T23:20:50.52Z
