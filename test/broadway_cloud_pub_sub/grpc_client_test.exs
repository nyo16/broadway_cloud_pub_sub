defmodule BroadwayCloudPubSub.GrpcClientTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.GrpcClient

  describe "init/1" do
    test "returns error when PubsubGrpc is not available" do
      # Since PubsubGrpc is not a dependency in tests, init should return error
      assert {:error, "PubsubGrpc library not available"} = GrpcClient.init([])
    end
  end

  describe "prepare_to_connect/2" do
    test "with GrpcClient as client module" do
      producer_opts = [client: GrpcClient, subscription: "test-sub"]
      
      {specs, updated_opts} = GrpcClient.prepare_to_connect(:test_name, producer_opts)
      
      # Should not add specs when PubsubGrpc is not available
      assert specs == []
      assert updated_opts == producer_opts
    end

    test "with GrpcClient and options as tuple" do
      producer_opts = [
        client: {GrpcClient, pool_size: 10}, 
        subscription: "test-sub"
      ]
      
      {specs, updated_opts} = GrpcClient.prepare_to_connect(:test_name, producer_opts)
      
      # Should not add specs when PubsubGrpc is not available
      assert specs == []
      assert updated_opts == producer_opts
    end

    test "with different client module" do
      producer_opts = [client: SomeOtherClient, subscription: "test-sub"]
      
      {specs, updated_opts} = GrpcClient.prepare_to_connect(:test_name, producer_opts)
      
      # Should return unchanged when not GrpcClient
      assert specs == []
      assert updated_opts == producer_opts
    end
  end

  describe "credential normalization" do
    test "normalize_credentials/1 handles different credential types" do
      # Use module function directly for testing
      assert :default == apply(GrpcClient, :normalize_credentials, [:default])
      assert :insecure == apply(GrpcClient, :normalize_credentials, [:insecure])
      assert {:goth, :test} == apply(GrpcClient, :normalize_credentials, [{:goth, :test}])
      assert {:token_generator, {M, :f, []}} == 
        apply(GrpcClient, :normalize_credentials, [{:token_generator, {M, :f, []}}])
    end
  end

  describe "message conversion" do
    test "extract_message_data/1 extracts data and metadata" do
      message_with_data = %{
        data: "test message",
        message_id: "123",
        publish_time: %{seconds: 1609459200, nanos: 0}
      }

      {data, metadata} = apply(GrpcClient, :extract_message_data, [message_with_data])
      
      assert data == "test message"
      assert metadata.message_id == "123"
      assert metadata.publish_time == %{seconds: 1609459200, nanos: 0}
      refute Map.has_key?(metadata, :data)
    end

    test "extract_message_data/1 handles message without data" do
      message_without_data = %{
        message_id: "123",
        publish_time: %{seconds: 1609459200, nanos: 0}
      }

      {data, metadata} = apply(GrpcClient, :extract_message_data, [message_without_data])
      
      assert data == nil
      assert metadata == message_without_data
    end

    test "parse_timestamp/1 handles different timestamp formats" do
      # Protobuf timestamp format
      proto_timestamp = %{seconds: 1609459200, nanos: 500_000_000}
      result = apply(GrpcClient, :parse_timestamp, [proto_timestamp])
      assert %DateTime{} = result
      assert result.year == 2021

      # ISO string format  
      iso_timestamp = "2021-01-01T00:00:00Z"
      result = apply(GrpcClient, :parse_timestamp, [iso_timestamp])
      assert %DateTime{} = result
      assert result.year == 2021

      # Nil timestamp
      assert nil == apply(GrpcClient, :parse_timestamp, [nil])

      # Invalid timestamp
      assert nil == apply(GrpcClient, :parse_timestamp, ["invalid"])
    end
  end
end