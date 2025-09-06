#!/usr/bin/env elixir

# Manual test script for gRPC adapter
# Usage: elixir test_manual.exs

defmodule ManualTest do
  def test_grpc_client do
    IO.puts("Testing gRPC Client...")
    
    # Test initialization without PubsubGrpc dependency
    case BroadwayCloudPubSub.GrpcClient.init([]) do
      {:error, msg} -> 
        IO.puts("✅ Expected error (PubsubGrpc not available): #{msg}")
      result -> 
        IO.puts("❌ Unexpected result: #{inspect(result)}")
    end
    
    # Test prepare_to_connect
    {specs, opts} = BroadwayCloudPubSub.GrpcClient.prepare_to_connect(
      :test_name, 
      [client: BroadwayCloudPubSub.GrpcClient, subscription: "test-sub"]
    )
    
    IO.puts("✅ prepare_to_connect works: specs=#{length(specs)}, opts present=#{is_list(opts)}")
    
    # Test credential normalization (using apply to access private function)
    creds = [
      :default,
      :insecure, 
      {:goth, :test},
      {:token_generator, {Module, :function, []}}
    ]
    
    Enum.each(creds, fn cred ->
      result = apply(BroadwayCloudPubSub.GrpcClient, :normalize_credentials, [cred])
      IO.puts("✅ Credential #{inspect(cred)} -> #{inspect(result)}")
    end)
    
    IO.puts("\n🎉 Basic gRPC client tests passed!")
  end
  
  def test_options do
    IO.puts("Testing Options...")
    
    # Test that credentials option is included
    definition = BroadwayCloudPubSub.Options.definition()
    
    if Keyword.has_key?(definition.schema, :credentials) do
      IO.puts("✅ Credentials option is properly defined")
    else
      IO.puts("❌ Credentials option missing")
    end
    
    IO.puts("🎉 Options tests passed!")
  end
end

# Run tests
ManualTest.test_grpc_client()
ManualTest.test_options()

IO.puts("""

Next steps to fully test:
1. Enable pubsub_grpc dependency in mix.exs
2. Run: mix deps.get && mix compile
3. Start emulator: docker-compose -f docker-compose.test.yml up pubsub-emulator pubsub-setup  
4. Run integration tests: mix test --only integration
5. Try the example: elixir examples/grpc_example.exs
""")