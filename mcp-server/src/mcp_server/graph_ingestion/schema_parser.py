import logging
from typing import Any, Dict, Optional

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


class SchemaParser:
    """Parses SQL DDL files to extract schema information."""

    def __init__(self, dialect: str = "postgres"):
        """Initialize the Schema Parser."""
        self.dialect = dialect

    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Read a SQL file and parse it into a structured schema dictionary."""
        try:
            with open(file_path, "r") as f:
                sql_content = f.read()

            return self.parse_sql(sql_content)
        except Exception as e:
            logger.error(f"Failed to parse file {file_path}: {e}")
            raise

    def parse_sql(self, sql_content: str) -> Dict[str, Any]:
        """Parse SQL content string and return schema structure."""
        parsed_schema: Dict[str, Any] = {"tables": []}

        try:
            # Parse all statements in the SQL file
            # Use error_level=IGNORE to skip over statements that sqlglot
            # fails to parse fully (like complex PL/PGSQL)
            statements = sqlglot.parse(
                sql_content, read=self.dialect, error_level=sqlglot.ErrorLevel.IGNORE
            )

            for statement in statements:
                if isinstance(statement, exp.Create) and statement.kind == "TABLE":
                    table_info = self._extract_table_info(statement)
                    if table_info:
                        parsed_schema["tables"].append(table_info)

            return parsed_schema

        except Exception as e:
            logger.error(f"Error parsing SQL content: {e}")
            raise

    def _extract_table_info(self, statement: exp.Create) -> Optional[Dict[str, Any]]:
        """Extract details from a CREATE TABLE statement."""
        # statement.this is typically a Schema object containing the
        # Table identifier and column definitions
        schema_node = statement.this

        if isinstance(schema_node, exp.Schema):
            # The table info is in schema_node.this
            table_node = schema_node.this
            expressions = schema_node.expressions
        else:
            # Fallback for simpler CREATE cases (though CREATE TABLE usually uses Schema)
            table_node = schema_node
            expressions = statement.expressions or []

        if isinstance(table_node, exp.Table):
            table_name = table_node.name
            schema_name = table_node.db
        else:
            table_name = table_node.name if hasattr(table_node, "name") else str(table_node)
            schema_name = None

        columns = []
        constraints = []

        # Iterate through table properties (columns and constraints)
        for expression in expressions:
            if isinstance(expression, exp.ColumnDef):
                columns.append(self._extract_column_info(expression))
            elif isinstance(expression, exp.Constraint):  # Table level constraints
                constraints.append(self._extract_constraint_info(expression))

        # Extract comments
        table_comment = statement.comments if hasattr(statement, "comments") else None

        return {
            "table_name": table_name,
            "schema": schema_name or "public",
            "columns": columns,
            "constraints": constraints,
            "comment": table_comment,
        }

    def _extract_column_info(self, expression: exp.ColumnDef) -> Dict[str, Any]:
        """Extract column details."""
        col_name = expression.this.name
        col_type = expression.kind.sql() if expression.kind else "UNKNOWN"

        # Check for inline constraints
        is_primary = False
        is_not_null = False
        default_value = None

        # In recent sqlglot, ColumnDef has 'constraints' list in args
        constraints = expression.args.get("constraints") or []

        for constraint in constraints:
            # constraint is usually exp.ColumnConstraint
            kind = constraint.kind

            if isinstance(kind, exp.PrimaryKeyColumnConstraint):
                is_primary = True
            elif isinstance(kind, exp.NotNullColumnConstraint):
                is_not_null = True
            elif isinstance(kind, exp.DefaultColumnConstraint):
                default_value = kind.this.sql()
            # Handle string based checks if specific classes are hard to import verify
            elif kind.key == "primary_key_column_constraint":
                is_primary = True
            elif kind.key == "not_null_column_constraint":
                is_not_null = True

        col_comment = expression.comments if hasattr(expression, "comments") else None

        return {
            "name": col_name,
            "type": col_type,
            "primary_key": is_primary,
            "not_null": is_not_null,
            "default": default_value,
            "comment": col_comment,
        }

    def _extract_constraint_info(self, expression: exp.Constraint) -> Dict[str, Any]:
        """Extract table-level constraint details."""
        # Basic extraction, can be expanded based on specific needs (FK, Check, etc)
        return {
            "type": expression.kind,
            "name": expression.this.name if expression.this else None,
            "definition": expression.sql(),
        }
