from node_controller.gateway.protos import gateway_pb2

StartService_input_indices = {
    1: gateway_pb2.Client,
    2: gateway_pb2.RecursionGuard,
    3: gateway_pb2.Configuration,
    4: gateway_pb2.celaut__pb2.Any.Metadata.HashTag.Hash,
    5: gateway_pb2.celaut__pb2.Any.Metadata,
    6: gateway_pb2.celaut__pb2.Service,
}
StartService_input_message_mode = {1: True, 2: True, 3: True, 4: True, 5: True, 6: False}  # False yield a Dir.
