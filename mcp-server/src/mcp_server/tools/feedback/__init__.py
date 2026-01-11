"""Feedback tools package for MCP server.

This package contains tools for submitting user feedback on query interactions.
"""

from mcp_server.tools.feedback.submit_feedback import handler as submit_feedback

__all__ = [
    "submit_feedback",
]
