import asyncio
import os
import sys
from typing import List, Tuple

from common.config.env import get_env_int, get_env_str


async def self_check():
    """Verify security posture of the Text2SQL system."""
    print("=== Text2SQL Security Self-Check ===")

    passed = True
    results: List[Tuple[str, bool, str]] = []

    # 1. Environment Variables
    budget = get_env_int("AGENT_TOKEN_BUDGET", 0)
    if budget > 0:
        results.append(("AGENT_TOKEN_BUDGET", True, f"Set to {budget}"))
    else:
        results.append(("AGENT_TOKEN_BUDGET", False, "Missing or 0 (High risk of budget runaway)"))
        passed = False

    role = get_env_str("MCP_USER_ROLE", "")
    if role:
        results.append(("MCP_USER_ROLE", True, f"Set to {role}"))
    else:
        results.append(("MCP_USER_ROLE", False, "Missing (Role validation might fail/default)"))
        passed = False

    # 2. Triple Lock - AST Level (Dry Run)
    from agent.validation.policy_enforcer import PolicyEnforcer

    try:
        PolicyEnforcer.validate_sql("UPDATE users SET admin=1")
        results.append(("AST Mutation Blocking", False, "FAILED: Allowed an UPDATE statement!"))
        passed = False
    except ValueError:
        results.append(("AST Mutation Blocking", True, "PASSED: Rejected mutation statement"))

    # 3. Triple Lock - MCP Client Level (Simulated)
    from mcp_server.tools.execute_sql_query import handler as execute_sql

    try:
        # Mocking tenant_id for skip
        res_json = await execute_sql(sql_query="DROP TABLE users", tenant_id=1)
        import json

        res = json.loads(res_json)
        if res.get("error"):
            results.append(("MCP Mutation Blocking", True, f"PASSED: {res['error']['message']}"))
        else:
            results.append(("MCP Mutation Blocking", False, "FAILED: Allowed DROP TABLE!"))
            passed = False
    except Exception as e:
        results.append(("MCP Mutation Blocking", False, f"FAILED: Error during check: {e}"))
        passed = False

    # 4. Admin Gating
    from mcp_server.tools.admin.reload_patterns import handler as reload_tools

    # Assuming role is NOT ADMIN or we mock it
    os.environ["MCP_USER_ROLE"] = "SQL_USER_ROLE"
    try:
        res_json = await reload_tools()
        res = json.loads(res_json)
        if res.get("error") and res["error"]["category"] == "auth":
            results.append(
                ("Admin Tool Gating", True, "PASSED: Rejected admin tool with user role")
            )
        elif res.get("error"):
            results.append(
                (
                    "Admin Tool Gating",
                    False,
                    f"FAILED: Unexpected error category: {res['error']['category']}",
                )
            )
            passed = False
        else:
            results.append(("Admin Tool Gating", False, "FAILED: Allowed admin tool access!"))
            passed = False
    except Exception as e:
        results.append(("Admin Tool Gating", False, f"FAILED: Error during check: {e}"))
        passed = False

    # Restore role if needed
    if role:
        os.environ["MCP_USER_ROLE"] = role

    # Print Report
    print(f"{'Check':<30} | {'Status':<10} | {'Details'}")
    print("-" * 70)
    for check, status, detail in results:
        status_str = "PASS" if status else "FAIL"
        print(f"{check:<30} | {status_str:<10} | {detail}")

    print("-" * 70)
    if passed:
        print("RESULT: SUCCESS - System hardening is ACTIVE.")
        sys.exit(0)
    else:
        print("RESULT: FAILURE - Security invariants violated!")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(self_check())
