"""Agent Visualization Module (Vega-Lite MVP).

This module provides utilities to generate Vega-Lite specifications from
tabular data returned by MCP SQL queries.

Design Notes:
-------------
Supported Chart Types:
  - Bar Charts: For categorical + numeric data (e.g., counts by category).
  - Line Charts: For temporal + numeric data (e.g., trends over time).
  - Scatter Plots: For numeric + numeric data (e.g., correlation).

Limitations (MVP):
  - Best-effort inference; returns None if data shape is complex or ambiguous.
  - No formatting (currency, percentages) or advanced interactivity.
  - No dashboarding; single chart per query result.
  - Cap on number of rows used for type inference to ensure performance.
"""
