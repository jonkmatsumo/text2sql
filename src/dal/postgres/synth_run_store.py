import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from common.interfaces.synth_run_store import SynthRunStore
from dal.control_plane import ControlPlaneDatabase
from dal.database import Database


class PostgresSynthRunStore(SynthRunStore):
    """Postgres implementation of SynthRunStore."""

    def _get_connection_context(self, tenant_id: Optional[int] = None):
        """Get the appropriate connection context based on isolation settings."""
        if ControlPlaneDatabase.is_enabled():
            return ControlPlaneDatabase.get_connection(tenant_id)
        return Database.get_connection(tenant_id)

    async def create_run(
        self,
        config_snapshot: Dict[str, Any],
        output_path: Optional[str] = None,
        status: str = "PENDING",
        job_id: Optional[UUID] = None,
    ) -> UUID:
        """Create a new synthetic generation run record."""
        async with self._get_connection_context() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO synth_generation_runs (config_snapshot, output_path, status, job_id)
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                json.dumps(config_snapshot),
                output_path,
                status,
                job_id,
            )
            return row["id"]

    async def update_run(
        self,
        run_id: UUID,
        status: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        manifest: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        ui_state: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update an existing synthetic generation run record."""
        async with self._get_connection_context() as conn:
            updates = []
            params = [run_id]
            p_idx = 2

            if status:
                updates.append(f"status = ${p_idx}")
                params.append(status)
                p_idx += 1
            if completed_at:
                updates.append(f"completed_at = ${p_idx}")
                params.append(completed_at)
                p_idx += 1
            if manifest:
                updates.append(f"manifest = ${p_idx}")
                params.append(json.dumps(manifest))
                p_idx += 1
            if metrics:
                updates.append(f"metrics = ${p_idx}")
                params.append(json.dumps(metrics))
                p_idx += 1
            if error_message:
                updates.append(f"error_message = ${p_idx}")
                params.append(error_message)
                p_idx += 1
            if ui_state:
                updates.append(f"ui_state = ${p_idx}")
                params.append(json.dumps(ui_state))
                p_idx += 1

            if not updates:
                return

            query = f"""
                UPDATE synth_generation_runs
                SET {", ".join(updates)}
                WHERE id = $1
            """
            await conn.execute(query, *params)

    async def get_run(self, run_id: UUID) -> Optional[Dict[str, Any]]:
        """Fetch a specific run record by ID."""
        async with self._get_connection_context() as conn:
            row = await conn.fetchrow("SELECT * FROM synth_generation_runs WHERE id = $1", run_id)
            if not row:
                return None

            # Convert Record to dict and handle JSON fields
            data = dict(row)
            for field in ["config_snapshot", "manifest", "metrics", "ui_state"]:
                if data.get(field) and isinstance(data[field], str):
                    try:
                        data[field] = json.loads(data[field])
                    except Exception:
                        pass
            return data

    async def list_runs(
        self, limit: int = 20, status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List recent synthetic generation runs."""
        async with self._get_connection_context() as conn:
            query = "SELECT * FROM synth_generation_runs"
            params = []
            if status:
                query += " WHERE status = $1"
                params.append(status)

            query += f" ORDER BY started_at DESC LIMIT ${len(params) + 1}"
            params.append(limit)

            rows = await conn.fetch(query, *params)

            result = []
            for row in rows:
                data = dict(row)
                for field in ["config_snapshot", "manifest", "metrics", "ui_state"]:
                    if data.get(field) and isinstance(data[field], str):
                        try:
                            data[field] = json.loads(data[field])
                        except Exception:
                            pass
                result.append(data)
            return result
