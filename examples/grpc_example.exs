#!/usr/bin/env elixir

# Example: Using BroadwayCloudPubSub with the new gRPC adapter
# Run with: elixir examples/grpc_example.exs

Mix.install([
  {:broadway_cloud_pub_sub, path: "."},
  {:pubsub_grpc, github: "nyo16/gcp_grpc_pubsub"}
])

defmodule OrderProcessor do
  use Broadway

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {BroadwayCloudPubSub.Producer, opts[:producer_opts]},
        stages: 2
      ],
      processors: [
        default: [stages: 4]
      ],
      batchers: [
        default: [
          batch_size: 10,
          batch_timeout: 2000,
          stages: 2
        ]
      ]
    )
  end

  @impl true
  def handle_message(_processor, message, context) do
    IO.puts("Processing message: #{inspect(message.data)}")
    
    # Parse JSON message data
    case Jason.decode(message.data) do
      {:ok, %{"type" => "order", "order_id" => order_id}} ->
        # Process order
        IO.puts("Processing order: #{order_id}")
        message

      {:ok, data} ->
        IO.puts("Processing other message: #{inspect(data)}")
        message

      {:error, _} ->
        # Invalid JSON, fail the message
        Broadway.Message.failed(message, "Invalid JSON")
    end
  end

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    IO.puts("Batch processed: #{length(messages)} messages")
    messages
  end
end

# Configuration examples:

# 1. Using gRPC with default authentication (GOOGLE_APPLICATION_CREDENTIALS)
grpc_config_default = [
  client: {BroadwayCloudPubSub.GrpcClient, pool_size: 5},
  credentials: :default,
  subscription: "projects/my-project/subscriptions/orders-subscription"
]

# 2. Using gRPC with Goth authentication
grpc_config_goth = [
  client: {BroadwayCloudPubSub.GrpcClient, pool_size: 10},
  credentials: {:goth, MyApp.Goth},
  subscription: "projects/my-project/subscriptions/orders-subscription"
]

# 3. Using gRPC with emulator (for testing)
grpc_config_emulator = [
  client: {BroadwayCloudPubSub.GrpcClient, 
    pool_size: 2,
    endpoint: "localhost:8085"
  },
  credentials: :insecure,
  subscription: "projects/test-project/subscriptions/test-subscription"
]

# 4. REST client for comparison (default)
rest_config = [
  client: BroadwayCloudPubSub.PullClient,
  goth: MyApp.Goth,
  subscription: "projects/my-project/subscriptions/orders-subscription"
]

IO.puts("""
Broadway Cloud Pub/Sub gRPC Adapter Example
==========================================

This example demonstrates how to use the new gRPC adapter for better performance.

Configuration examples:

1. gRPC with default auth:
#{inspect(grpc_config_default, pretty: true)}

2. gRPC with Goth:
#{inspect(grpc_config_goth, pretty: true)}

3. gRPC with emulator:
#{inspect(grpc_config_emulator, pretty: true)}

4. REST client (default):
#{inspect(rest_config, pretty: true)}

To start the processor with gRPC:

OrderProcessor.start_link(producer_opts: grpc_config_default)

For testing with the emulator:

1. Start emulator:
   docker-compose -f docker-compose.test.yml up pubsub-emulator pubsub-setup

2. Run with emulator config:
   OrderProcessor.start_link(producer_opts: grpc_config_emulator)

Performance benefits of gRPC:
- HTTP/2 multiplexing
- Binary protocol (smaller payloads)
- Connection pooling
- Lower latency

See EMULATOR_TESTING.md for comprehensive testing instructions.
""")

# Uncomment to actually start the processor:
# {:ok, _pid} = OrderProcessor.start_link(producer_opts: grpc_config_emulator)
# Process.sleep(:infinity)