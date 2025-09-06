# BroadwayCloudPubSub

[![CI](https://github.com/dashbitco/broadway_cloud_pub_sub/actions/workflows/ci.yml/badge.svg)](https://github.com/dashbitco/broadway_cloud_pub_sub/actions/workflows/ci.yml)

A Google Cloud Pub/Sub connector for [Broadway](https://github.com/dashbitco/broadway).

Documentation can be found at [https://hexdocs.pm/broadway_cloud_pub_sub](https://hexdocs.pm/broadway_cloud_pub_sub).

This project provides:

* `BroadwayCloudPubSub.Producer` - A GenStage producer that continuously receives messages from a Pub/Sub subscription acknowledges them after being successfully processed.
* `BroadwayCloudPubSub.Client` - A generic behaviour to implement Pub/Sub clients.
* `BroadwayCloudPubSub.PullClient` - Default REST client used by `BroadwayCloudPubSub.Producer`.
* `BroadwayCloudPubSub.GrpcClient` - High-performance gRPC client with connection pooling and binary protocol support.

## Installation

Add `:broadway_cloud_pub_sub` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 0.9.0"},
    {:goth, "~> 1.3"}
  ]
end
```

> Note the [goth](https://hexdocs.pm/goth) package, which handles Google Authentication, is required for the default token generator.

For high-performance scenarios using the gRPC client, also add:

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 0.9.0"},
    {:goth, "~> 1.3"},
    {:pubsub_grpc, "~> 0.1.0"}
  ]
end
```

## Usage

Configure Broadway with one or more producers using `BroadwayCloudPubSub.Producer`:

### REST Client (Default)

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Producer,
      goth: MyGoth,
      subscription: "projects/my-project/subscriptions/my-subscription"
    }
  ]
)
```

### gRPC Client (High Performance)

For better performance, especially with high message volumes, use the gRPC client:

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Producer,
      client: {BroadwayCloudPubSub.GrpcClient, pool_size: 10},
      credentials: {:goth, MyGoth},
      subscription: "projects/my-project/subscriptions/my-subscription"
    }
  ]
)
```

The gRPC client provides:
- **Better Performance**: HTTP/2 multiplexing and binary protocol
- **Connection Pooling**: Configurable pool size for concurrent connections  
- **Lower Latency**: Persistent connections reduce connection overhead
- **Smaller Payloads**: 30-60% smaller message payloads vs REST

## Testing

See [EMULATOR_TESTING.md](EMULATOR_TESTING.md) for comprehensive testing with the Google Cloud Pub/Sub Emulator, including:
- Docker Compose setup for local development
- End-to-end integration tests with realistic scenarios
- Performance comparison between REST and gRPC clients
- CI/CD integration examples

## License

Copyright 2019 Michael Crumm \
Copyright 2020 Dashbit

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
