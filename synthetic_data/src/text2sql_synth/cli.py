"""CLI entrypoint for text2sql-synth."""

import argparse
import logging
import sys
from pathlib import Path

from text2sql_synth.config import ScalePreset, SynthConfig
from text2sql_synth.export import export_to_directory
from text2sql_synth.orchestrator import generate_tables

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def resolve_config(args: argparse.Namespace) -> SynthConfig:
    """Resolve configuration from either --preset or --config.

    Args:
        args: Parsed command line arguments.

    Returns:
        Resolved SynthConfig instance.

    Raises:
        SystemExit: If neither or both --preset and --config are specified.
    """
    if args.preset and args.config:
        print("Error: Cannot specify both --preset and --config", file=sys.stderr)
        sys.exit(1)

    if args.preset:
        return SynthConfig.preset(args.preset)

    if args.config:
        config_path = Path(args.config)
        if config_path.suffix in (".yaml", ".yml"):
            return SynthConfig.from_yaml(config_path)
        elif config_path.suffix == ".json":
            return SynthConfig.from_json(config_path)
        else:
            # Try YAML first, then JSON
            try:
                return SynthConfig.from_yaml(config_path)
            except Exception:
                return SynthConfig.from_json(config_path)

    print("Error: Must specify either --preset or --config", file=sys.stderr)
    sys.exit(1)


def cmd_generate(args: argparse.Namespace) -> int:
    """Generate synthetic data from a configuration file or preset."""
    try:
        config = resolve_config(args)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    logger.info("Configuration resolved.")

    try:
        # Process --only to handle commas if provided
        only_tables = None
        if args.only:
            only_tables = []
            for item in args.only:
                if "," in item:
                    only_tables.extend(i.strip() for i in item.split(",") if i.strip())
                else:
                    only_tables.append(item.strip())

        # Run generation
        ctx, tables = generate_tables(config, only=only_tables)

        # Export results
        manifest_path = export_to_directory(ctx, config, args.out)

        logger.info("Generation and export successful!")

        # Print summary
        print("\n" + "=" * 40)
        print("SYNTHETIC DATA GENERATION SUMMARY")
        print("=" * 40)
        print(f"Output Directory: {args.out}")
        print(f"Manifest:         {manifest_path}")
        print(f"Seed:             {config.seed}")
        print(f"Tables Generated: {len(tables)}")
        print("-" * 40)
        for table_name, df in sorted(tables.items()):
            print(f"- {table_name:25} {len(df):>8} rows")
        print("=" * 40 + "\n")

        return 0
    except Exception as e:
        logger.exception("Failed to generate data: %s", e)
        return 1


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate a generated manifest."""
    from text2sql_synth.validate import validate_manifest

    logger.info(f"Starting validation for manifest: {args.manifest}")
    result = validate_manifest(args.manifest)

    if result.is_valid:
        print("\n✅ Validation PASSED")
    else:
        print("\n❌ Validation FAILED")
        for error in result.errors:
            print(f"  - {error}")

    print(f"\nReport written to {Path(args.manifest).parent / 'validation_report.md'}")

    return 0 if result.is_valid else 1


def cmd_load_postgres(args: argparse.Namespace) -> int:
    """Load generated data into a Postgres database."""
    from text2sql_synth.loaders.postgres import load_from_manifest

    try:
        load_from_manifest(
            args.manifest,
            args.dsn,
            target_schema=args.schema,
            table_prefix=args.prefix,
            truncate=not args.no_truncate,
        )
        return 0
    except Exception as e:
        logger.exception("Failed to load data into Postgres: %s", e)
        return 1


def cmd_export_sql(args: argparse.Namespace) -> int:
    """Export SQL files for compose bootstrap."""
    from text2sql_synth.sql_export import export_sql_files

    try:
        config = resolve_config(args)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    logger.info("Configuration resolved.")

    try:
        # Run generation
        ctx, tables = generate_tables(config)

        # Export SQL files
        paths = export_sql_files(ctx, args.out, target_schema=args.schema)

        logger.info("SQL export successful!")

        # Print summary
        print("\n" + "=" * 50)
        print("SYNTHETIC DATA SQL EXPORT")
        print("=" * 50)
        print(f"Output Directory: {args.out}")
        print(f"Schema File:      {paths['schema']}")
        print(f"Data File:        {paths['data']}")
        print(f"Tables Exported:  {len(tables)}")
        print("-" * 50)
        print("")
        print("To use with Docker Compose, copy files to:")
        print("  database/query-target/")
        print("")
        print("Files will be executed in order when container starts.")
        print("=" * 50 + "\n")

        return 0
    except Exception as e:
        logger.exception("Failed to export SQL: %s", e)
        return 1


def cmd_export_tables_json(args: argparse.Namespace) -> int:
    """Export tables.json."""
    import json

    from text2sql_synth.tables_summary import generate_tables_summary

    try:
        data = generate_tables_summary()
        output_path = Path(args.out) / "tables.json"

        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Exported tables.json to {output_path}")
        print(f"Generated summaries for {len(data)} tables.")
        return 0
    except Exception as e:
        logger.exception("Failed to export tables.json: %s", e)
        return 1


def cmd_export_examples(args: argparse.Namespace) -> int:
    """Export few-shot examples."""
    import json

    from text2sql_synth.examples_generator import generate_examples

    try:
        examples = generate_examples()
        output_dir = Path(args.out)

        # Ensure directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        for filename, content in examples.items():
            output_path = output_dir / filename
            with open(output_path, "w") as f:
                json.dump(content, f, indent=2)

        print(f"Exported {len(examples)} example files to {output_dir}")
        return 0
    except Exception as e:
        logger.exception("Failed to export examples: %s", e)
        return 1


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="text2sql-synth",
        description="Deterministic synthetic data generation for text2sql testing",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # generate subcommand
    gen_parser = subparsers.add_parser(
        "generate",
        help="Generate synthetic data from a configuration file or preset",
    )
    config_group = gen_parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument(
        "--config",
        metavar="PATH",
        help="Path to the generation configuration file (YAML or JSON)",
    )
    config_group.add_argument(
        "--preset",
        choices=[p.value for p in ScalePreset],
        help="Use a built-in preset (small, mvp, or medium)",
    )
    gen_parser.add_argument(
        "--out",
        required=True,
        metavar="DIR",
        help="Output directory for generated data",
    )
    gen_parser.add_argument(
        "--only",
        nargs="+",
        metavar="TABLE",
        help="Only generate specific tables and their dependencies",
    )
    gen_parser.set_defaults(func=cmd_generate)

    # load-postgres subcommand
    load_parser = subparsers.add_parser(
        "load-postgres",
        help="Load generated data into a Postgres database",
    )
    load_parser.add_argument(
        "--manifest",
        required=True,
        metavar="PATH",
        help="Path to the manifest.json file",
    )
    load_parser.add_argument(
        "--dsn",
        required=True,
        metavar="DSN",
        help="Postgres connection string (DSN)",
    )
    load_parser.add_argument(
        "--schema",
        default="public",
        metavar="NAME",
        help="Target schema in Postgres (default: public)",
    )
    load_parser.add_argument(
        "--prefix",
        default="",
        metavar="STR",
        help="Optional prefix for table names",
    )
    load_parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Disable truncation of tables before loading",
    )
    load_parser.set_defaults(func=cmd_load_postgres)

    # validate subcommand
    val_parser = subparsers.add_parser(
        "validate",
        help="Validate a generated manifest",
    )
    val_parser.add_argument(
        "--manifest",
        required=True,
        help="Path to the manifest file to validate",
    )
    val_parser.set_defaults(func=cmd_validate)

    # export-sql subcommand
    sql_parser = subparsers.add_parser(
        "export-sql",
        help="Export SQL files for compose bootstrap (01-schema.sql, 02-data.sql)",
    )
    sql_config_group = sql_parser.add_mutually_exclusive_group(required=True)
    sql_config_group.add_argument(
        "--config",
        metavar="PATH",
        help="Path to the generation configuration file (YAML or JSON)",
    )
    sql_config_group.add_argument(
        "--preset",
        choices=[p.value for p in ScalePreset],
        help="Use a built-in preset (small, mvp, or medium)",
    )
    sql_parser.add_argument(
        "--out",
        required=True,
        metavar="DIR",
        help="Output directory for SQL files",
    )
    sql_parser.add_argument(
        "--schema",
        default="public",
        metavar="NAME",
        help="Target schema in SQL statements (default: public)",
    )
    sql_parser.set_defaults(func=cmd_export_sql)

    # export-tables-json subcommand
    json_parser = subparsers.add_parser(
        "export-tables-json",
        help="Export tables.json containing table summaries",
    )
    json_parser.add_argument(
        "--out",
        required=True,
        metavar="DIR",
        help="Output directory for tables.json",
    )
    json_parser.set_defaults(func=cmd_export_tables_json)

    # export-examples subcommand
    ex_parser = subparsers.add_parser(
        "export-examples",
        help="Export few-shot example JSON files",
    )
    ex_parser.add_argument(
        "--out",
        required=True,
        metavar="DIR",
        help="Output directory for example files",
    )
    ex_parser.set_defaults(func=cmd_export_examples)

    return parser


def main() -> int:
    """Run the CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
