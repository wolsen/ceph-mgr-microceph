# SPDX-FileCopyrightText: 2025 - Canonical Ltd
# SPDX-License-Identifier: Apache-2.0

import json
from unittest.mock import Mock, MagicMock, patch
import pytest

from ceph.deployment.service_spec import RGWSpec, NFSServiceSpec, PlacementSpec, HostPlacementSpec


class TestMicroCephOrchestrator:
    """Tests for MicroCeph orchestrator module."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock MicroCeph client."""
        client = Mock()
        client.services = Mock()
        client.services.enable_service = Mock()
        client.status = Mock()
        client.status.is_available = Mock()
        return client

    @pytest.fixture
    def orchestrator(self, mock_client):
        """Create a MicroCeph orchestrator instance with mocked client."""
        with patch('microceph.module.Client.from_socket', return_value=mock_client):
            from microceph.module import MicroCephOrchestrator
            orch = MicroCephOrchestrator('mgr_module', 0, 0)
            orch.microceph = mock_client
            return orch

    def test_apply_rgw_basic(self, orchestrator, mock_client):
        """Test basic RGW service creation without placement."""
        spec = RGWSpec(service_id='rgw.test')
        
        result = orchestrator.apply_rgw(spec)
        
        # Verify enable_service was called with correct parameters
        mock_client.services.enable_service.assert_called_once_with('rgw')
        assert 'enabled successfully' in str(result.result)

    def test_apply_rgw_with_placement(self, orchestrator, mock_client):
        """Test RGW service creation with host placement."""
        placement = PlacementSpec(hosts=[HostPlacementSpec(hostname='node1')])
        spec = RGWSpec(service_id='rgw.test', placement=placement)
        
        result = orchestrator.apply_rgw(spec)
        
        # Verify enable_service was called with target host
        mock_client.services.enable_service.assert_called_once_with('rgw', target='node1')
        assert 'enabled successfully' in str(result.result)

    def test_apply_rgw_with_port(self, orchestrator, mock_client):
        """Test RGW service creation with custom port."""
        spec = RGWSpec(service_id='rgw.test', rgw_frontend_port=8080)
        
        result = orchestrator.apply_rgw(spec)
        
        # Verify enable_service was called with port
        mock_client.services.enable_service.assert_called_once_with('rgw', port=8080)
        assert 'enabled successfully' in str(result.result)

    def test_apply_rgw_with_placement_and_port(self, orchestrator, mock_client):
        """Test RGW service creation with both placement and port."""
        placement = PlacementSpec(hosts=[HostPlacementSpec(hostname='node1')])
        spec = RGWSpec(service_id='rgw.test', placement=placement, rgw_frontend_port=8080)
        
        result = orchestrator.apply_rgw(spec)
        
        # Verify enable_service was called with both target and port
        mock_client.services.enable_service.assert_called_once_with('rgw', target='node1', port=8080)
        assert 'enabled successfully' in str(result.result)

    def test_apply_nfs_basic(self, orchestrator, mock_client):
        """Test basic NFS service creation without placement."""
        spec = NFSServiceSpec(service_id='nfs.test')
        
        result = orchestrator.apply_nfs(spec)
        
        # Verify enable_service was called with correct parameters
        mock_client.services.enable_service.assert_called_once_with('nfs')
        assert 'enabled successfully' in str(result.result)

    def test_apply_nfs_with_placement(self, orchestrator, mock_client):
        """Test NFS service creation with host placement."""
        placement = PlacementSpec(hosts=[HostPlacementSpec(hostname='node2')])
        spec = NFSServiceSpec(service_id='nfs.test', placement=placement)
        
        result = orchestrator.apply_nfs(spec)
        
        # Verify enable_service was called with target host
        mock_client.services.enable_service.assert_called_once_with('nfs', target='node2')
        assert 'enabled successfully' in str(result.result)


class TestExtendedAPIService:
    """Tests for ExtendedAPIService client."""

    def test_enable_service_basic(self):
        """Test enable_service method sends correct API request."""
        from microceph.client.cluster import ExtendedAPIService
        
        # Create mock session and endpoint
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"metadata": {}}
        mock_session.request.return_value = mock_response
        
        service = ExtendedAPIService(mock_session, "http://test", None)
        
        # Call enable_service
        service.enable_service('rgw')
        
        # Verify POST request was made with correct data
        mock_session.request.assert_called_once()
        args, kwargs = mock_session.request.call_args
        assert args[0] == 'post'
        assert '/1.0/services' in args[1]
        assert 'data' in kwargs
        data = json.loads(kwargs['data'])
        assert data['service'] == 'rgw'

    def test_enable_service_with_params(self):
        """Test enable_service method with additional parameters."""
        from microceph.client.cluster import ExtendedAPIService
        
        # Create mock session and endpoint
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"metadata": {}}
        mock_session.request.return_value = mock_response
        
        service = ExtendedAPIService(mock_session, "http://test", None)
        
        # Call enable_service with parameters
        service.enable_service('rgw', port=8080, target='node1')
        
        # Verify POST request includes the parameters
        args, kwargs = mock_session.request.call_args
        data = json.loads(kwargs['data'])
        assert data['service'] == 'rgw'
        assert data['port'] == 8080
        assert data['target'] == 'node1'
