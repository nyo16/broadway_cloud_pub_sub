defmodule BroadwayCloudPubSub.GrpcClient do
  @moduledoc """
  A gRPC-based client for Google Cloud Pub/Sub built on `PubsubGrpc`.

  This client provides better performance than the REST-based `PullClient` by using
  gRPC's streaming capabilities and binary protocol. It supports:

  - Connection pooling with configurable pool size
  - Multiple authentication methods (Service Account, Goth)  
  - Automatic connection recovery
  - Binary protocol for reduced payload size

  ## Options

  All options from `BroadwayCloudPubSub.Producer` are supported, plus:

    * `:pool_size` - Number of gRPC connections in the pool (default: `5`)
    * `:endpoint` - Custom gRPC endpoint, useful for emulator testing
    * `:credentials` - Authentication credentials:
      - `:default` - Uses `GOOGLE_APPLICATION_CREDENTIALS` environment variable
      - `{:goth, goth_name}` - Uses the specified Goth server
      - `{:token_generator, {module, function, args}}` - Custom token generator

  ## Examples

  Using default authentication:

      producer: [
        module: {BroadwayCloudPubSub.Producer,
          client: BroadwayCloudPubSub.GrpcClient,
          subscription: "projects/my-project/subscriptions/my-subscription"
        }
      ]

  Using custom pool size and Goth authentication:

      producer: [
        module: {BroadwayCloudPubSub.Producer,
          client: {BroadwayCloudPubSub.GrpcClient, pool_size: 10},
          credentials: {:goth, MyApp.Goth},
          subscription: "projects/my-project/subscriptions/my-subscription"
        }
      ]

  Using with Pub/Sub emulator:

      producer: [
        module: {BroadwayCloudPubSub.Producer,
          client: {BroadwayCloudPubSub.GrpcClient, 
            endpoint: "localhost:8085", 
            credentials: :insecure
          },
          subscription: "projects/test-project/subscriptions/test-subscription"
        }
      ]
  """

  alias Broadway.Message
  alias BroadwayCloudPubSub.Client

  require Logger

  @behaviour Client

  @default_pool_size 5
  @default_endpoint "pubsub.googleapis.com:443"

  @impl Client
  def prepare_to_connect(name, producer_opts) do
    case Keyword.get(producer_opts, :client) do
      {__MODULE__, client_opts} ->
        prepare_grpc_pool(name, client_opts, producer_opts)

      __MODULE__ ->
        prepare_grpc_pool(name, [], producer_opts)

      _ ->
        {[], producer_opts}
    end
  end

  defp prepare_grpc_pool(name, client_opts, producer_opts) do
    pool_name = Module.concat(name, __MODULE__)
    pool_size = Keyword.get(client_opts, :pool_size, @default_pool_size)
    endpoint = Keyword.get(client_opts, :endpoint, @default_endpoint)
    credentials = Keyword.get(producer_opts, :credentials, :default)

    grpc_opts = [
      endpoint: endpoint,
      credentials: normalize_credentials(credentials),
      pool_size: pool_size
    ]

    specs = [
      {PubsubGrpc, [name: pool_name] ++ grpc_opts}
    ]

    producer_opts = 
      producer_opts
      |> Keyword.put(:grpc_pool, pool_name)
      |> Keyword.merge(client_opts)

    {specs, producer_opts}
  end

  defp normalize_credentials(:default), do: :default
  defp normalize_credentials(:insecure), do: :insecure
  defp normalize_credentials({:goth, goth_name}), do: {:goth, goth_name}
  defp normalize_credentials({:token_generator, mfa}), do: {:token_generator, mfa}

  @impl Client
  def init(opts) do
    unless Code.ensure_loaded?(PubsubGrpc) do
      Logger.error("""
      the gRPC client requires the PubsubGrpc library but it's not available

      Add pubsub_grpc to your dependencies:

          defp deps do
            [{:pubsub_grpc, "~> 0.1.0"}]
          end

      Or use the default REST client:

          client: BroadwayCloudPubSub.PullClient
      """)

      {:error, "PubsubGrpc library not available"}
    else
      {:ok, Map.new(opts)}
    end
  end

  @impl Client
  def receive_messages(demand, ack_builder, config) do
    max_messages = min(demand, config.max_number_of_messages)
    
    :telemetry.span(
      [:broadway_cloud_pub_sub, :grpc_client, :receive_messages],
      %{
        max_messages: max_messages,
        demand: demand,
        name: config.broadway[:name]
      },
      fn ->
        result =
          config
          |> pull_messages(max_messages)
          |> handle_response(:receive_messages)
          |> wrap_received_messages(ack_builder)

        {result, %{name: config.broadway[:name], max_messages: max_messages, demand: demand}}
      end
    )
  end

  @impl Client
  def acknowledge(ack_ids, config) do
    :telemetry.span(
      [:broadway_cloud_pub_sub, :grpc_client, :ack],
      %{name: config.topology_name},
      fn ->
        result =
          config
          |> ack_messages(ack_ids)
          |> handle_response(:acknowledge)

        {result, %{name: config.topology_name}}
      end
    )
  end

  @impl Client  
  def put_deadline(ack_ids, ack_deadline_seconds, config) do
    config
    |> modify_ack_deadline(ack_ids, ack_deadline_seconds)
    |> handle_response(:put_deadline)
  end

  defp pull_messages(config, max_messages) do
    PubsubGrpc.pull_messages(
      config.grpc_pool,
      config.subscription,
      max_messages: max_messages
    )
  end

  defp ack_messages(config, ack_ids) do
    PubsubGrpc.acknowledge(
      config.grpc_pool,
      config.subscription,
      ack_ids
    )
  end

  defp modify_ack_deadline(config, ack_ids, ack_deadline_seconds) do
    PubsubGrpc.modify_ack_deadline(
      config.grpc_pool,
      config.subscription,
      ack_ids,
      ack_deadline_seconds
    )
  end

  defp handle_response({:ok, messages}, :receive_messages) when is_list(messages) do
    messages
  end

  defp handle_response({:ok, %{received_messages: messages}}, :receive_messages) do
    messages
  end

  defp handle_response({:ok, _}, _) do
    :ok
  end

  defp handle_response({:error, reason}, :receive_messages) do
    Logger.error("Unable to fetch events from Cloud Pub/Sub via gRPC - reason: #{inspect(reason)}")
    []
  end

  defp handle_response({:error, reason}, :acknowledge) do
    Logger.error("Unable to acknowledge messages with Cloud Pub/Sub via gRPC - reason: #{inspect(reason)}")
    :ok
  end

  defp handle_response({:error, reason}, :put_deadline) do
    Logger.error("Unable to put new ack deadline with Cloud Pub/Sub via gRPC - reason: #{inspect(reason)}")
    :ok
  end

  defp wrap_received_messages(grpc_messages, ack_builder) do
    Enum.map(grpc_messages, fn grpc_msg ->
      grpc_msg_to_broadway_msg(grpc_msg, ack_builder)
    end)
  end

  defp grpc_msg_to_broadway_msg(%{ack_id: ack_id, message: message} = grpc_msg, ack_builder) do
    delivery_attempt = Map.get(grpc_msg, :delivery_attempt)

    {data, metadata} = extract_message_data(message)

    metadata = %{
      attributes: metadata.attributes || %{},
      deliveryAttempt: delivery_attempt,
      messageId: metadata.message_id,
      orderingKey: metadata.ordering_key,
      publishTime: parse_timestamp(metadata.publish_time)
    }

    %Message{
      data: data,
      metadata: metadata,
      acknowledger: ack_builder.(ack_id)
    }
  end

  defp extract_message_data(%{data: data} = message) do
    metadata = Map.drop(message, [:data])
    {data, metadata}
  end

  defp extract_message_data(message) do
    {nil, message}
  end

  defp parse_timestamp(nil), do: nil
  defp parse_timestamp(%{seconds: seconds, nanos: nanos}) do
    DateTime.from_unix!(seconds, :second)
    |> DateTime.add(div(nanos, 1_000_000), :millisecond)
  end
  defp parse_timestamp(timestamp) when is_binary(timestamp) do
    case DateTime.from_iso8601(timestamp) do
      {:ok, dt, _} -> dt
      _ -> nil
    end
  end
  defp parse_timestamp(_), do: nil
end