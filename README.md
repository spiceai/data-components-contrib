# data-components-contrib

[![build](https://github.com/spiceai/data-components-contrib/actions/workflows/build.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/data-components-contrib/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.com/channels/803820740868571196/803820740868571199)
[![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)

Community built data connectors and processors for [Spice.ai](https://spiceai.org)

The vision for **data-components-contrib** is a community-driven library of components for streaming and processing time series data for use in the Spice.ai runtime. Spice.ai provides a general interface that anyone can implement to create data connectors and data processors. This enables both authors and consumers of Spice.ai pods, to use the same data sources for training and inferencing ML models. Join us in helping make AI easy for developers.

See [CONTRIBUTING.md](CONTRIBUTING.md) on how you can contribute a component.

## Data Component Concepts

### Dataspace

A [dataspace](https://docs.spiceai.org/reference/pod#dataspaces") is a specification on how the Spice.ai runtime and AI engine loads, processes and interacts with data from a single source. A dataspace may contain a single data connector and data processor. There may be multiple dataspace definitions within a pod. The fields specified in the union of dataspaces are used as inputs to the neural networks that Spice.ai trains.

### Data Connector

A [data connector](https://docs.spiceai.org/reference/pod#data-connector) is a reuseable component that contains logic to fetch or ingest data from an external source.

Learn more at [Data Connectors](dataconnectors/README.md)

### Data Processor

A [data processor](https://docs.spiceai.org/reference/pod##data-processor">}}) is a reusable component, composable with a data connector that contains logic to process raw connector data into [observations](https://docs.spiceai.org/api#observations">) and state Spice.ai can use.

Learn more at [Data Processors](dataprocessors/README.md)
