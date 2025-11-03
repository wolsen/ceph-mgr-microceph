#
# Copyright 2025, Canonical Ltd.
#


import logging
from typing import Tuple, Dict, Any, List, Optional
from collections import defaultdict
import time

from ceph.deployment.inventory import Device, Devices
from ceph.deployment.service_spec import (
    ServiceSpec,
    PlacementSpec,
    HostPlacementSpec,
    IngressSpec,
    RGWSpec,
    MONSpec,
    MDSSpec, NFSServiceSpec,
)

from mgr_module import MgrModule
from mgr_module import NotifyType

from orchestrator import (
    Orchestrator,
    HostSpec,
    InventoryFilter,
    InventoryHost,
    ServiceDescription,
    DaemonDescription,
    CLICommandMeta,
    handle_orch_error,
    OrchResult,
)

from .client.client import Client
from .client.service import RemoteException

logger = logging.getLogger(__name__)


class MicroCephOrchestrator(Orchestrator,
                            MgrModule,
                            metaclass=CLICommandMeta):

    def __init__(self, *args: Any, **kwargs: Any):
        """

        :param args:
        :param kwargs:
        """
        super(MicroCephOrchestrator, self).__init__(*args, **kwargs)
        self.microceph = Client.from_socket()
        self.run = True

    def serve(self) -> None:
        """
        Called by the ceph-mgr service to start any server that
        is provided by this Python plugin.  The implementation
        of this function should block until ``shutdown`` is called.

        You *must* implement ``shutdown`` if you implement ``serve``

        :return:
        """
        while self.run:
            logger.debug("Running serve loop")
            time.sleep(30)

    def shutdown(self) -> None:
        """

        :return:
        """
        self.run = False

    def available(self) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Report whether we can talk to the orchestrator.  This is the
        place to give the user a meaningful message if the orchestrator
        isn't running or can't be contacted.

        This method may be called frequently (e.g. every page load
        to conditionally display a warning banner), so make sure it's
        not too expensive.  It's okay to give a slightly stale status
        (e.g. based on a periodic background ping of the orchestrator)
        if that's necessary to make this method fast.

        .. note::
            `True` doesn't mean that the desired functionality
            is actually available in the orchestrator. I.e. this
            won't work as expected::

                >>> #doctest: +SKIP
                ... if OrchestratorClientMixin().available()[0]:  # wrong.
                ...     OrchestratorClientMixin().get_hosts()

        :return: boolean representing whether the module is available/usable
        :return: string describing any error
        :return: dict containing any module specific information
        """
        try:
            self.microceph.status.is_available()
        except RemoteException as e:
            return False, f"Cannot reach the MicroCeph API: {e}", {}

        return True, "", {}

    def notify(self, notify_type: NotifyType, notify_id: str) -> None:
        """

        :param notify_type:
        :param notify_id:
        :return:
        """
        logger.info(f"notify called with notify_type: {notify_type} and notify_id: {notify_id}")

    @handle_orch_error
    def get_hosts(self) -> List[HostSpec]:
        """
        Report the hosts in the cluster.

        :return: list of HostSpec
        """
        return [
            HostSpec(m['name'], m['address'], status=m['status'])
            for m in self.microceph.cluster.get_cluster_members()
        ]

    @handle_orch_error
    def describe_service(self,
                service_type: Optional[str] = None,
                service_name: Optional[str] = None,
                refresh: bool = False
            ) -> List[ServiceDescription]:

        logger.info(f"describing service... service_type={service_type}, service_name={service_name}, "
                    f"refresh={refresh}")

        services = self.microceph.services.list_services()
        service_map = defaultdict(list)
        for svc in services:
            service_map[svc['service']].append(svc['location'])

        service_descs = []
        for service, locations in service_map.items():
            spec = None
            if service == 'mon':
                spec = MONSpec('mon', count=len(locations))
            elif service == 'mds':
                spec = MDSSpec('mds')
            elif service == 'rgw':
                spec = RGWSpec('rgw')
            else:
                spec = ServiceSpec(service_type=service, count=len(locations))

            service_descs.append(ServiceDescription(
                spec=spec
            ))

        return service_descs

    @handle_orch_error
    def list_daemons(self,
                service_name: Optional[str] = None,
                daemon_type: Optional[str] = None,
                daemon_id: Optional[str] = None,
                host: Optional[str] = None,
                refresh: bool = False
            ) -> List[DaemonDescription]:

        logger.info(f"listing daemons... service_name={service_name}, daemon_type={daemon_type}, "
                    f"daemon_id={daemon_id}, host={host}, refresh={refresh}")

        services = self.microceph.services.list_services()
        descriptions = []
        for svc in services:
            daemon_type = svc['service']
            hostname = svc['location']
            descriptions.append(DaemonDescription(
                daemon_type=daemon_type,
                daemon_id=hostname,
                hostname=hostname
            ))
        return descriptions

    def get_inventory(self,
                host_filter: Optional[InventoryFilter] = None,
                refresh: bool = False
            ) -> OrchResult[List[InventoryHost]]:

        disks = self.microceph.services.list_disks()
        disks_by_host = defaultdict(list)
        for d in disks:
            disks_by_host[d['location']].append(
                Device(path=d['path'])
            )

        inventory = []
        for host, diskettes in disks_by_host.items():
            inventory.append(InventoryHost(
                name=host,
                devices=Devices(diskettes)
            ))

        return OrchResult(inventory)

    @handle_orch_error
    def apply_rgw(self, spec: RGWSpec) -> OrchResult[str]:
        """
        Apply RGW service specification by enabling RGW service in MicroCeph.

        :param spec: RGW service specification
        :return: Result indicating success
        """
        logger.info(f"Applying RGW service with spec: {spec}")
        
        # Extract parameters from spec
        kwargs = {}
        
        # Set target host if placement is specified
        if spec.placement and spec.placement.hosts:
            # Use the first host from the placement spec
            kwargs['target'] = spec.placement.hosts[0].hostname
        
        # Set port if specified in spec
        if hasattr(spec, 'rgw_frontend_port') and spec.rgw_frontend_port:
            kwargs['port'] = spec.rgw_frontend_port
        
        # Enable the RGW service via MicroCeph API
        self.microceph.services.enable_service('rgw', **kwargs)
        
        return OrchResult(f"RGW service {spec.service_id} enabled successfully")

    @handle_orch_error
    def apply_nfs(self, spec: NFSServiceSpec) -> OrchResult[str]:
        """
        Apply NFS service specification by enabling NFS service in MicroCeph.

        :param spec: NFS service specification
        :return: Result indicating success
        """
        logger.info(f"Applying NFS service with spec: {spec}")
        
        # Extract parameters from spec
        kwargs = {}
        
        # Set target host if placement is specified
        if spec.placement and spec.placement.hosts:
            # Use the first host from the placement spec
            kwargs['target'] = spec.placement.hosts[0].hostname
        
        # Enable the NFS service via MicroCeph API
        self.microceph.services.enable_service('nfs', **kwargs)
        
        return OrchResult(f"NFS service {spec.service_id} enabled successfully")
