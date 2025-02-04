from datetime import timedelta, datetime
from threading import Thread, Lock
from time import sleep
from typing import Dict, Callable, Any, Tuple, Union, Optional

from node_controller.dependency_manager.service_interface import ServiceInterface
from node_controller.dependency_manager.service_instance import ServiceInstance
from node_controller.dependency_manager.service_config import ServiceConfig
from node_controller.gateway.communication import generate_gateway_stub
from node_controller.gateway.protos import gateway_pb2, celaut_pb2, gateway_pb2_grpc
from node_controller.utils.lambdas import SHA3_256, STATIC_SERVICE_DIRECTORY, DYNAMIC_SERVICE_DIRECTORY, \
    STATIC_METADATA_DIRECTORY, DYNAMIC_METADATA_DIRECTORY
from node_controller.utils.lambdas import LOGGER
from node_controller.utils.singleton import Singleton

MAINTENANCE_SLEEP_TIME_DEFAULT = 60
TIMEOUT_DEFAULT = 30
FAILED_ATTEMPTS_DEFAULT = 20
PASS_TIMEOUT_TIMES_DEFAULT = 5


class DependencyManager(metaclass=Singleton):

    def __init__(self,
                 node_url: str = "",
                 static_service_directory: str = STATIC_SERVICE_DIRECTORY,
                 static_metadata_directory: str = STATIC_METADATA_DIRECTORY,
                 dynamic_service_directory: str = DYNAMIC_SERVICE_DIRECTORY,
                 dynamic_metadata_directory: str = DYNAMIC_METADATA_DIRECTORY,
                 maintenance_sleep_time: int = MAINTENANCE_SLEEP_TIME_DEFAULT,
                 timeout: int = TIMEOUT_DEFAULT,
                 failed_attempts: int = FAILED_ATTEMPTS_DEFAULT,
                 pass_timeout_times: int = PASS_TIMEOUT_TIMES_DEFAULT,
                 dev_client: str = None,
                 ):

        if not node_url:
            raise Exception("Node url not provided.")

        self.maintenance_sleep_time = maintenance_sleep_time
        self.timeout = timeout
        self.failed_attempts = failed_attempts
        self.pass_timeout_times = pass_timeout_times

        self.dev_client = dev_client
        self.static_service_directory = static_service_directory
        self.static_metadata_directory = static_metadata_directory
        self.dynamic_service_directory = dynamic_service_directory
        self.dynamic_metadata_directory = dynamic_metadata_directory

        self.services: Dict[str, ServiceConfig] = {}
        self.gateway_stub: gateway_pb2_grpc.GatewayStub = generate_gateway_stub(node_url)

        self.lock = Lock()
        Thread(target=self.maintenance, name='DependencyMaintainer').start()

    def maintenance(self):
        while True:
            sleep(self.maintenance_sleep_time)
            index = 0
            while True:  # If we do for service in services, the entire loop would need to be blocked.
                LOGGER('maintainer want services lock' + str(self.lock.locked()))
                self.lock.acquire()

                try:
                    service_config: ServiceConfig = self.services[
                        list(self.services)[index]
                    ]
                    index += 1
                    try:
                        instance: ServiceInstance = service_config.get_instance(deep=True)

                    except IndexError:
                        # There are no available instances in this queue.
                        self.lock.release()
                        continue
                except IndexError:
                    LOGGER('All services have been toured.')
                    self.lock.release()
                    break
                except Exception as e:
                    LOGGER('ERROR on maintainer, ' + str(e))
                    self.lock.release()
                    break
                self.lock.release()

                LOGGER('      maintain service instance --> ' + str(instance))
                # In case it has gone unused for too long or is in a 'zombie' state.
                if datetime.now() - instance.use_datetime > timedelta(
                        minutes=self.maintenance_sleep_time) \
                        or instance.is_zombie(
                    pass_timeout_times=service_config.pass_timeout_times,
                    timeout=service_config.timeout,
                    failed_attempts=service_config.failed_attempts
                ):
                    instance.stop(self.gateway_stub)
                # Otherwise, add the instance back to its respective queue.
                else:
                    self.lock.acquire()
                    service_config.add_instance(instance, deep=True)
                    self.lock.release()

    def add_service(self,
                    service_hash: str,
                    config: Optional[celaut_pb2.Configuration] = None,
                    dynamic: bool = False,
                    timeout: int = None,
                    failed_attempts: int = None,
                    pass_timeout_times: int = None
                    ) -> ServiceInterface:

        if not config:
            config = celaut_pb2.Configuration()

        service_config_id: str = SHA3_256(
            bytes(service_hash, 'utf-8') + SHA3_256(
                config.SerializeToString()
            )
        ).hex()
        with self.lock:
            service_config: ServiceConfig = ServiceConfig(
                service_hash=service_hash,
                config=config,
                timeout=timeout if timeout else self.timeout,
                failed_attempts=failed_attempts if failed_attempts else self.failed_attempts,
                pass_timeout_times=pass_timeout_times if pass_timeout_times else self.pass_timeout_times,
                dynamic=dynamic,
                dev_client=self.dev_client,
                static_service_directory=self.static_service_directory,
                static_metadata_directory=self.static_metadata_directory,
                dynamic_service_directory=self.dynamic_service_directory,
                dynamic_metadata_directory=self.dynamic_metadata_directory
            )
            self.services.update({
                service_config_id: service_config
            })

        return ServiceInterface(
            service_with_config=service_config,
            gateway_stub=self.gateway_stub
        )

    def get_service_with_config(self, service_config_id: str, mem_manager: Callable[[int], Any]) \
            -> Tuple[
                Union[str, celaut_pb2.Service],
                Union[str, celaut_pb2.Any.Metadata],
                gateway_pb2.Configuration]:
        raise Exception("Not implemented.")
        # return self.services[service_config_id].get_service_with_config(mem_manager=mem_manager)
