# Coinbase Data Connector

The Coinbase data connector uses Coinbase Pro exchange data to stream exchange ticker data.

The connector uses the [WebSocket Feed](https://docs.cloud.coinbase.com/exchange/docs/overview) and reads real-time price updates every time a match happens for a set of products. It uses the WebSocket [ticker channel](https://docs.cloud.coinbase.com/exchange/docs/channels#the-ticker-channel).

The feed returns data in JSON form, so the connector should be paired with the [json data processor](../../dataprocessors/json/README.md).

```json
{
  "type": "ticker",
  "trade_id": 20153558,
  "sequence": 3262786978,
  "time": "2017-09-02T17:05:49.250000Z",
  "product_id": "BTC-USD",
  "price": "4388.01000000",
  "side": "buy", // taker side
  "last_size": "0.03000000",
  "best_bid": "4388",
  "best_ask": "4388.01"
}
```

## Supported parameters

- `product_ids` A comma-delimited list of Coinbase Pro supported product ids. E.g. `BTC-USD,ETH-USD`.

## Example Dataspace

```yaml
dataspaces:
  - from: coinbase
    name: btcusd
    fields:
      - name: price
      - name: side
        type: tag
      - name: last_size
      - name: best_bid
      - name: best_ask
    data:
      connector:
        name: coinbase
        params:
          product_ids: BTC-USD
      processor:
        name: json
```
