"""Unit tests for OpsService."""

import pytest
from unittest.mock import patch

try:
    from service.ops_service import OpsService
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from service.ops_service import OpsService


@pytest.mark.asyncio
async def test_run_pattern_generation():
    """Test calling pattern generation through OpsService."""
    with patch("mcp_server.services.ops.maintenance.MaintenanceService.generate_patterns") as mock_gen:
        async def mock_iter(dry_run):
            yield "Log 1"
            yield "Log 2"
        mock_gen.side_effect = mock_iter
        
        logs = []
        async for log in OpsService.run_pattern_generation(dry_run=True):
            logs.append(log)
            
        assert len(logs) == 2
        assert mock_gen.called


@pytest.mark.asyncio
async def test_run_schema_hydration():
    """Test calling schema hydration through OpsService."""
    with patch("mcp_server.services.ops.maintenance.MaintenanceService.hydrate_schema") as mock_gen:
        async def mock_iter():
            yield "Log H"
        mock_gen.side_effect = mock_iter
        
        logs = []
        async for log in OpsService.run_schema_hydration():
            logs.append(log)
            
        assert logs == ["Log H"]
        assert mock_gen.called
