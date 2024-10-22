import logging
from node_controller.dependency_manager.dependency_manager import DependencyManager
from node_controller.gateway.communication import modify_resources
from node_controller.gateway.protos import celaut_pb2
from node_controller.utils.get_grpc_uri import get_grpc_uri
from node_controller.utils.read_file import read_file

from node_controller.utils.singleton import Singleton
from resource_manager.resourcemanager import ResourceManager


class Controller(metaclass=Singleton):

    def __init__(self, default_log=True, default_dependency_manager=True, default_resource_manager=True):
        config = celaut_pb2.ConfigurationFile()
        config.ParseFromString(
            read_file('/__config__')
        )

        gateway_uri = get_grpc_uri(config.gateway)
        self.mem_limit: int = config.initial_sysresources.mem_limit
        self.node_url = f"{gateway_uri.ip}:{str(gateway_uri.port)}"

        if default_log:
            logging.basicConfig(
                filename='app.log',
                level=logging.DEBUG,
                format='%(asctime)s - %(levelname)s - %(message)s'
            )

        if default_dependency_manager:
            DependencyManager(
                node_url=self.node_url,
                maintenance_sleep_time=60,
                timeout=30,
                failed_attempts=3,
                pass_timeout_times=5,
                dev_client=None,
                static_service_directory="__services__",
                static_metadata_directory="__metadata__",
                dynamic_service_directory="",
                dynamic_metadata_directory=""
            )

        if default_resource_manager:
            ResourceManager(
                log=lambda message: logging.info(message),
                ram_pool_method=lambda: self.mem_limit,
                modify_resources=lambda d: modify_resources(i=d, node_url=self.node_url)
            )

    def get_node_url(self) -> str:
        return self.node_url

    def get_mem_limit_at_start(self) -> int:
        return self.mem_limit
