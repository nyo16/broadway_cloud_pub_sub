# Google Cloud Pub/Sub Emulator Testing

This document explains how to test the Broadway Cloud Pub/Sub library (including the new gRPC adapter) using the Google Cloud Pub/Sub Emulator.

## Overview

The Google Cloud Pub/Sub Emulator allows you to develop and test your applications locally without connecting to the actual Google Cloud Pub/Sub service. This is particularly useful for:

- **Development**: Test your Broadway pipelines without cloud dependencies
- **CI/CD**: Run integration tests in automated environments  
- **Performance Testing**: Benchmark different client implementations (REST vs gRPC)
- **Cost Optimization**: Avoid Pub/Sub charges during development

## Quick Start with Docker

The easiest way to get started is using the provided Docker Compose configuration:

```bash
# Start the emulator and run integration tests
docker-compose -f docker-compose.test.yml up test-runner

# Or start just the emulator for manual testing
docker-compose -f docker-compose.test.yml up pubsub-emulator pubsub-setup
```

This will:
1. Start the Pub/Sub emulator on port 8085
2. Create test topics and subscriptions
3. Run the integration test suite

## Manual Setup

### Prerequisites

- Google Cloud SDK installed (`gcloud`)
- Elixir/Erlang environment
- Docker (optional, for containerized testing)

### 1. Install and Start the Emulator

```bash
# Install the emulator component
gcloud components install pubsub-emulator

# Start the emulator
gcloud beta emulators pubsub start --project=test-project --host-port=localhost:8085
```

### 2. Set Environment Variables

In a new terminal, configure the environment:

```bash
# Point clients to the emulator
export PUBSUB_EMULATOR_HOST=localhost:8085
export CLOUDSDK_CORE_PROJECT=test-project
```

### 3. Create Topics and Subscriptions

```bash
# Create test topics
gcloud pubsub topics create events-topic
gcloud pubsub topics create orders-topic
gcloud pubsub topics create metrics-topic

# Create subscriptions
gcloud pubsub subscriptions create events-subscription --topic=events-topic
gcloud pubsub subscriptions create orders-subscription --topic=orders-topic  
gcloud pubsub subscriptions create metrics-subscription --topic=metrics-topic
```

## Testing Both Client Types

### REST Client (Default)

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Producer,
      # Uses REST API client (default)
      client: BroadwayCloudPubSub.PullClient,
      subscription: "projects/test-project/subscriptions/events-subscription",
      base_url: "http://localhost:8085",  # Point to emulator
      # No authentication needed for emulator
      token_generator: fn -> {:ok, "fake-token"} end
    }
  ],
  processors: [default: []],
  batchers: [default: [batch_size: 10]]
)
```

### gRPC Client (New)

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Producer,
      # Uses gRPC client for better performance
      client: {BroadwayCloudPubSub.GrpcClient, 
        pool_size: 5,
        endpoint: "localhost:8085"  # Point to emulator
      },
      credentials: :insecure,  # No auth for emulator
      subscription: "projects/test-project/subscriptions/events-subscription"
    }
  ],
  processors: [default: []],
  batchers: [default: [batch_size: 10]]
)
```

## Integration Test Examples

The integration tests demonstrate realistic scenarios:

### Order Processing Pipeline

```elixir
# Tests order processing with revenue tracking
test "end-to-end order processing pipeline with gRPC client" do
  # Uses atomic counters to track:
  # - Number of orders processed
  # - Total revenue calculated
  # - Batches processed
  
  orders_processed = :atomics.get(:orders_counter, 1)
  total_revenue = :atomics.get(:revenue_counter, 1)
  
  assert orders_processed == 5
  assert total_revenue == 825
end
```

### Metrics Aggregation

```elixir  
# Tests metrics collection using ETS tables
test "metrics aggregation with atomic counters" do
  # Publishes CPU, memory, disk metrics
  # Aggregates values in ETS table
  # Verifies correct totals
  
  cpu_total = :ets.lookup(:metrics_table, "cpu_usage")
  assert cpu_total == 253  # Sum of all CPU readings
end
```

### Error Handling

```elixir
# Tests error handling and message acknowledgment
test "error handling and message acknowledgment" do  
  # Mix of successful and failing messages
  # Verifies only successful messages are counted
  # Tests acknowledgment behavior
  
  events_processed = :atomics.get(:events_counter, 1)
  assert events_processed == 2  # Only successful events
end
```

## Running Tests

### Local Testing

```bash
# Set environment variables
export PUBSUB_EMULATOR_HOST=localhost:8085

# Run integration tests
mix test --only integration

# Run specific test
mix test test/integration/grpc_emulator_test.exs
```

### Docker Testing

```bash
# Run all tests in Docker environment
docker-compose -f docker-compose.test.yml up test-runner

# Clean up
docker-compose -f docker-compose.test.yml down -v
```

### Continuous Integration

Add to your CI pipeline:

```yaml
# GitHub Actions example
- name: Start Pub/Sub Emulator
  run: |
    docker-compose -f docker-compose.test.yml up -d pubsub-emulator pubsub-setup
    sleep 10  # Wait for setup to complete

- name: Run Integration Tests  
  run: |
    export PUBSUB_EMULATOR_HOST=localhost:8085
    mix test --only integration
  env:
    MIX_ENV: test
```

## Performance Comparison

Use the emulator to compare REST vs gRPC client performance:

### Benchmarking Script

```elixir
# test/benchmark/client_comparison.exs
defmodule ClientBenchmark do
  def run_benchmark do
    # Test with different message volumes
    [100, 1000, 5000, 10000]
    |> Enum.each(fn message_count ->
      IO.puts("Testing #{message_count} messages...")
      
      # Benchmark REST client
      rest_time = benchmark_client(BroadwayCloudPubSub.PullClient, message_count)
      
      # Benchmark gRPC client  
      grpc_time = benchmark_client(BroadwayCloudPubSub.GrpcClient, message_count)
      
      improvement = (rest_time - grpc_time) / rest_time * 100
      IO.puts("gRPC was #{:erlang.float_to_binary(improvement, decimals: 1)}% faster")
    end)
  end
  
  defp benchmark_client(client_module, message_count) do
    # Implementation details...
  end
end
```

## Troubleshooting

### Common Issues

1. **Connection Refused**: Ensure emulator is running on correct port
   ```bash
   curl http://localhost:8085/v1/projects/test-project/topics
   ```

2. **Topics Not Found**: Make sure setup script ran successfully
   ```bash
   gcloud pubsub topics list --project=test-project
   ```

3. **Authentication Errors**: Use `:insecure` credentials for emulator
   ```elixir
   credentials: :insecure
   ```

4. **gRPC Connection Issues**: Verify endpoint format
   ```elixir
   endpoint: "localhost:8085"  # Correct
   endpoint: "http://localhost:8085"  # Wrong for gRPC
   ```

### Debug Mode

Enable debug logging:

```elixir
# In config/test.exs
config :logger, level: :debug

# In your Broadway setup
config :broadway_cloud_pub_sub, :debug, true
```

### Monitoring

Monitor emulator activity:

```bash
# Watch emulator logs
docker-compose -f docker-compose.test.yml logs -f pubsub-emulator

# Check subscription stats
gcloud pubsub subscriptions describe events-subscription \
  --project=test-project
```

## Best Practices

1. **Use Realistic Data**: Test with realistic message sizes and volumes
2. **Test Both Clients**: Compare REST vs gRPC performance in your use case
3. **Batch Configuration**: Test different batch sizes and timeouts
4. **Error Scenarios**: Include failure cases in your tests
5. **Cleanup**: Reset state between tests using atomic counters/ETS
6. **Isolation**: Use separate topics/subscriptions per test when needed

## Advanced Scenarios

### Message Ordering

```elixir
# Test ordered message processing
gcloud pubsub subscriptions create ordered-subscription \
  --topic=events-topic \
  --enable-message-ordering
```

### Dead Letter Topics

```elixir
# Create dead letter topic and test failure handling  
gcloud pubsub topics create dead-letter-topic

gcloud pubsub subscriptions create main-subscription \
  --topic=events-topic \
  --dead-letter-topic=dead-letter-topic \
  --max-delivery-attempts=3
```

### High Volume Testing

```elixir
# Test with high message volumes
defmodule HighVolumeTest do
  @messages_per_batch 1000
  @total_batches 100
  
  test "high volume processing" do
    # Generate and publish large number of messages
    # Monitor memory usage and processing latency
    # Compare client performance under load
  end
end
```

This setup provides a comprehensive testing environment for both development and CI/CD, allowing you to thoroughly test the gRPC adapter alongside the existing REST client.