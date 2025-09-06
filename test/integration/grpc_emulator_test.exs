defmodule BroadwayCloudPubSub.Integration.GrpcEmulatorTest do
  @moduledoc """
  End-to-end integration tests for the gRPC client using Google Cloud Pub/Sub Emulator.
  
  These tests simulate realistic scenarios such as:
  - Order processing pipeline with event counters
  - Metrics aggregation using atomic counters
  - Message ordering and batching
  - Error handling and retries
  
  Run with: `mix test --only integration`
  or with Docker: `docker-compose -f docker-compose.test.yml up test-runner`
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  alias Broadway.Message

  # Test configuration
  @emulator_host System.get_env("PUBSUB_EMULATOR_HOST", "localhost:8085")
  @project_id "test-project"

  defmodule TestBroadway do
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
            batch_timeout: 1000,
            stages: 2
          ]
        ]
      )
    end

    @impl true
    def handle_message(_processor, message, %{test_pid: test_pid} = _context) do
      send(test_pid, {:message_received, message})
      
      # Simulate processing based on message data
      case Jason.decode!(message.data) do
        %{"type" => "order", "order_id" => order_id, "amount" => amount} ->
          # Increment order counter
          :atomics.add_get(:orders_counter, 1, 1)
          :atomics.add_get(:revenue_counter, 1, amount)
          
          Message.put_batcher(message, :default)
          
        %{"type" => "metric", "metric_name" => metric_name, "value" => value} ->
          # Track metrics in ETS
          :ets.update_counter(:metrics_table, metric_name, value, {metric_name, 0})
          message
          
        %{"type" => "event", "event_name" => event_name} ->
          # Simple event counting
          :atomics.add_get(:events_counter, 1, 1)
          message
          
        %{"error" => true} ->
          # Simulate processing error
          Message.failed(message, "Simulated processing error")
          
        _ ->
          message
      end
    end

    @impl true
    def handle_batch(:default, messages, _batch_info, %{test_pid: test_pid}) do
      # Simulate batch processing (e.g., bulk database insert)
      batch_size = length(messages)
      :atomics.add_get(:batches_counter, 1, 1)
      :atomics.add_get(:batch_size_counter, 1, batch_size)
      
      send(test_pid, {:batch_processed, batch_size})
      messages
    end
  end

  setup_all do
    # Create atomic counters for test metrics
    orders_counter = :atomics.new(1, signed: false)
    revenue_counter = :atomics.new(1, signed: false)
    events_counter = :atomics.new(1, signed: false)
    batches_counter = :atomics.new(1, signed: false)
    batch_size_counter = :atomics.new(1, signed: false)
    
    # Create ETS table for metrics
    :ets.new(:metrics_table, [:named_table, :public, :set])
    
    # Store counters in persistent term for access across processes
    :persistent_term.put(:orders_counter, orders_counter)
    :persistent_term.put(:revenue_counter, revenue_counter)
    :persistent_term.put(:events_counter, events_counter)
    :persistent_term.put(:batches_counter, batches_counter)
    :persistent_term.put(:batch_size_counter, batch_size_counter)
    
    on_exit(fn ->
      :persistent_term.erase(:orders_counter)
      :persistent_term.erase(:revenue_counter)
      :persistent_term.erase(:events_counter)
      :persistent_term.erase(:batches_counter)
      :persistent_term.erase(:batch_size_counter)
      :ets.delete(:metrics_table)
    end)
    
    :ok
  end

  setup do
    # Reset counters
    :atomics.put(:orders_counter, 1, 0)
    :atomics.put(:revenue_counter, 1, 0)
    :atomics.put(:events_counter, 1, 0)
    :atomics.put(:batches_counter, 1, 0)
    :atomics.put(:batch_size_counter, 1, 0)
    
    # Clear ETS table
    :ets.delete_all_objects(:metrics_table)
    
    :ok
  end

  @tag timeout: 60_000
  test "end-to-end order processing pipeline with gRPC client" do
    subscription = "projects/#{@project_id}/subscriptions/orders-subscription"
    topic = "projects/#{@project_id}/topics/orders-topic"
    
    # Start Broadway with gRPC client
    {:ok, broadway_pid} = TestBroadway.start_link(
      producer_opts: [
        client: {BroadwayCloudPubSub.GrpcClient, 
          pool_size: 2,
          endpoint: @emulator_host
        },
        credentials: :insecure,
        subscription: subscription,
        max_number_of_messages: 5,
        receive_interval: 1000,
        broadway: [name: TestBroadway],
        test_pid: self()
      ]
    )
    
    # Publish test orders
    orders = [
      %{type: "order", order_id: "order-001", amount: 99, customer: "john@example.com"},
      %{type: "order", order_id: "order-002", amount: 149, customer: "jane@example.com"},
      %{type: "order", order_id: "order-003", amount: 79, customer: "bob@example.com"},
      %{type: "order", order_id: "order-004", amount: 199, customer: "alice@example.com"},
      %{type: "order", order_id: "order-005", amount: 299, customer: "charlie@example.com"}
    ]
    
    # Publish orders using gRPC (simulate external system)
    publish_messages(topic, orders)
    
    # Wait for messages to be processed
    Process.sleep(5000)
    
    # Verify counters
    orders_processed = :atomics.get(:orders_counter, 1)
    total_revenue = :atomics.get(:revenue_counter, 1)
    batches_processed = :atomics.get(:batches_counter, 1)
    
    assert orders_processed == 5
    assert total_revenue == 825  # Sum of all order amounts
    assert batches_processed >= 1
    
    # Verify we received individual messages
    received_messages = receive_messages_until_count(5, 10_000)
    assert length(received_messages) == 5
    
    # Verify message content
    for message <- received_messages do
      data = Jason.decode!(message.data)
      assert data["type"] == "order"
      assert is_binary(data["order_id"])
      assert is_number(data["amount"])
    end
    
    GenServer.stop(broadway_pid)
  end

  @tag timeout: 30_000
  test "metrics aggregation with atomic counters" do
    subscription = "projects/#{@project_id}/subscriptions/metrics-subscription"
    topic = "projects/#{@project_id}/topics/metrics-topic"
    
    {:ok, broadway_pid} = TestBroadway.start_link(
      producer_opts: [
        client: {BroadwayCloudPubSub.GrpcClient, endpoint: @emulator_host},
        credentials: :insecure,
        subscription: subscription,
        max_number_of_messages: 10,
        receive_interval: 500,
        broadway: [name: TestBroadway],
        test_pid: self()
      ]
    )
    
    # Publish various metrics
    metrics = [
      %{type: "metric", metric_name: "cpu_usage", value: 85},
      %{type: "metric", metric_name: "memory_usage", value: 72},
      %{type: "metric", metric_name: "cpu_usage", value: 90},
      %{type: "metric", metric_name: "disk_usage", value: 45},
      %{type: "metric", metric_name: "memory_usage", value: 68},
      %{type: "metric", metric_name: "cpu_usage", value: 78}
    ]
    
    publish_messages(topic, metrics)
    
    # Wait for processing
    Process.sleep(3000)
    
    # Verify metrics aggregation in ETS
    cpu_total = case :ets.lookup(:metrics_table, "cpu_usage") do
      [{"cpu_usage", value}] -> value
      [] -> 0
    end
    
    memory_total = case :ets.lookup(:metrics_table, "memory_usage") do
      [{"memory_usage", value}] -> value
      [] -> 0
    end
    
    disk_total = case :ets.lookup(:metrics_table, "disk_usage") do
      [{"disk_usage", value}] -> value
      [] -> 0
    end
    
    assert cpu_total == 253  # 85 + 90 + 78
    assert memory_total == 140  # 72 + 68
    assert disk_total == 45
    
    GenServer.stop(broadway_pid)
  end

  @tag timeout: 30_000
  test "event processing with simple counters" do
    subscription = "projects/#{@project_id}/subscriptions/events-subscription"
    topic = "projects/#{@project_id}/topics/events-topic"
    
    {:ok, broadway_pid} = TestBroadway.start_link(
      producer_opts: [
        client: {BroadwayCloudPubSub.GrpcClient, endpoint: @emulator_host},
        credentials: :insecure,
        subscription: subscription,
        max_number_of_messages: 3,
        receive_interval: 1000,
        broadway: [name: TestBroadway],
        test_pid: self()
      ]
    )
    
    # Publish various events
    events = [
      %{type: "event", event_name: "user_login", user_id: "user123"},
      %{type: "event", event_name: "page_view", page: "/dashboard", user_id: "user123"},
      %{type: "event", event_name: "user_logout", user_id: "user123"},
      %{type: "event", event_name: "user_login", user_id: "user456"}
    ]
    
    publish_messages(topic, events)
    
    # Wait for processing
    Process.sleep(3000)
    
    # Verify event counter
    events_processed = :atomics.get(:events_counter, 1)
    assert events_processed == 4
    
    GenServer.stop(broadway_pid)
  end

  @tag timeout: 20_000
  test "error handling and message acknowledgment" do
    subscription = "projects/#{@project_id}/subscriptions/events-subscription"  
    topic = "projects/#{@project_id}/topics/events-topic"
    
    {:ok, broadway_pid} = TestBroadway.start_link(
      producer_opts: [
        client: {BroadwayCloudPubSub.GrpcClient, endpoint: @emulator_host},
        credentials: :insecure,
        subscription: subscription,
        max_number_of_messages: 5,
        receive_interval: 1000,
        broadway: [name: TestBroadway],
        test_pid: self(),
        on_failure: :noop  # Don't acknowledge failed messages
      ]
    )
    
    # Mix of good and bad messages
    messages = [
      %{type: "event", event_name: "good_event_1"},
      %{error: true, type: "bad_event"},  # This will cause processing error
      %{type: "event", event_name: "good_event_2"},
      %{error: true, type: "another_bad_event"}  # This will also fail
    ]
    
    publish_messages(topic, messages)
    
    # Wait for processing
    Process.sleep(3000)
    
    # Only successful events should be counted (2 good events)
    events_processed = :atomics.get(:events_counter, 1)
    assert events_processed == 2
    
    # Should have received all messages for processing attempt
    received_messages = receive_messages_until_count(4, 5_000)
    assert length(received_messages) == 4
    
    GenServer.stop(broadway_pid)
  end

  # Helper functions
  
  defp publish_messages(topic, messages) do
    # Use PubsubGrpc to publish messages (simulating external publisher)
    messages_data = Enum.map(messages, fn msg ->
      %{data: Jason.encode!(msg), attributes: %{"source" => "test"}}
    end)
    
    # Note: This assumes PubsubGrpc is available and configured for emulator
    # In a real test environment, you might use the gcloud CLI or HTTP API
    case Code.ensure_loaded?(PubsubGrpc) do
      true ->
        try do
          PubsubGrpc.publish(:test_pool, topic, messages_data)
        rescue
          _ ->
            # Fallback to HTTP API if gRPC publish fails
            publish_via_http(topic, messages_data)
        end
      false ->
        publish_via_http(topic, messages_data)
    end
  end
  
  defp publish_via_http(topic, messages_data) do
    # Extract project and topic name from full topic path
    [_, project, "topics", topic_name] = String.split(topic, "/")
    
    url = "http://#{@emulator_host}/v1/projects/#{project}/topics/#{topic_name}:publish"
    
    payload = %{
      messages: Enum.map(messages_data, fn msg ->
        %{
          data: Base.encode64(msg.data),
          attributes: msg.attributes || %{}
        }
      end)
    }
    
    headers = [{"content-type", "application/json"}]
    body = Jason.encode!(payload)
    
    case HTTPoison.post(url, body, headers, recv_timeout: 5000) do
      {:ok, %{status_code: 200}} -> :ok
      {:ok, response} -> {:error, "HTTP #{response.status_code}"}
      {:error, reason} -> {:error, reason}
    end
  end
  
  defp receive_messages_until_count(expected_count, timeout) do
    receive_messages_until_count(expected_count, timeout, [])
  end
  
  defp receive_messages_until_count(expected_count, timeout, acc) when length(acc) >= expected_count do
    Enum.take(acc, expected_count)
  end
  
  defp receive_messages_until_count(expected_count, timeout, acc) when timeout <= 0 do
    acc
  end
  
  defp receive_messages_until_count(expected_count, timeout, acc) do
    start_time = :erlang.monotonic_time(:millisecond)
    
    receive do
      {:message_received, message} ->
        new_acc = [message | acc]
        remaining_time = timeout - (:erlang.monotonic_time(:millisecond) - start_time)
        receive_messages_until_count(expected_count, remaining_time, new_acc)
    after
      min(timeout, 1000) ->
        remaining_time = timeout - (:erlang.monotonic_time(:millisecond) - start_time)
        receive_messages_until_count(expected_count, remaining_time, acc)
    end
  end
end