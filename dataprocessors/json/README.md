# JSON Processor

To use the JSON Processor use this data config:

```yaml
data:
  processor:
    name: json
```

## Params

| Name          | Supported Values                                            | Description                                        |
| ------------- | ----------------------------------------------------------- | -------------------------------------------------- |
| time_format   | [A Golang time.Parse layout](https://pkg.go.dev/time#Parse) | Specifies the format of the time field for parsing |
| time_selector | A top-level field name                                      | Specifies the field to use for time                |

## Dataspace Config

In the current version, the JSON Processor supports a flat object structure. Fields are mapped into Observations using the `measurements`, `categories` and `tags` nodes. E.g.

```yaml
dataspaces:
  - from: event
    name: stream
    data:
      processor:
        name: json
    measurements:
      - name: speed
      - name: height
      - name: target
    categories:
      - name: rating
        values:
          - 1
          - 2
          - 3
          - 4
          - 5
    tags:
      selectors:
        - tags
      values:
        - tagA
        - tagB
```

An example input payload is as follows:

```json
[
  {
    "time": 1631499271,
    "rating": 4,
    "speed": 26,
    "height": 81,
    "target": 12,
    "tags": ["tagA", "tagB"]
  }
]
```

## Datatypes

- The value of `time` must either be a Unix timestamp or a string conforming to RFC3339 i.e. 1985-04-12T23:20:50.52Z or a custom time format must be passed in the `time_format` param.

- Measurements must be JSON numbers.

- Categories must be strings.

- When using `tags` or `_tags` selectors, the tags field is parsed as an array of tag values. Any other field assumes a single string tag value.
