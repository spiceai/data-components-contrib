# JSON Processor

To use the JSON Processor use this data config:

```yaml
data:
  processor:
    name: json
```

By default the JSON processor will use the [Spice.ai observation json format](https://github.com/spiceai/spiceai/blob/trunk/pkg/api/observation/observation_schema.json).

An example payload with that schema looks like:

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

## More formats

To extend Spice.ai to accept a new JSON format, fork this repo and create a folder in this directory. Implement this interface:

```golang
type JsonFormat interface {
	GetSchema() *[]byte
	GetObservations(data *[]byte) ([]observations.Observation, error)
	GetState(data *[]byte, validFields *[]string) ([]*state.State, error)
}
```

**GetSchema()** should return the [JSON Schema](https://json-schema.org/) of the new format, which is used during validation.

**GetObservations()** returns the observations that Spice.ai will send to the AI Engine. Observations will only be for a single dataspace.

**GetState()** returns the state that Spice.ai will send to the AI Engine.
