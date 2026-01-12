"""CLI entrypoint for text2sql-synth."""

import argparse
import sys
import logging
from pathlib import Path

from text2sql_synth.config import ScalePreset, SynthConfig
from text2sql_synth.orchestrator import generate_all
from text2sql_synth.export import export_to_directory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
        # Run generation
        ctx = generate_all(config)
        
        # Export results
        manifest_path = export_to_directory(ctx, config, args.out)
        
        logger.info("Generation and export successful!")
        logger.info("Manifest: %s", manifest_path)
        return 0
    except Exception as e:
        logger.exception("Failed to generate data: %s", e)
        return 1


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate a generated manifest."""
    print(f"validate: manifest={args.manifest}")
    print("NOT IMPLEMENTED")
    return 1


def cmd_load_postgres(args: argparse.Namespace) -> int:
    """Load data from a manifest into PostgreSQL."""
    print(f"load-postgres: manifest={args.manifest}, dsn={args.dsn}")
    print("NOT IMPLEMENTED")
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
    gen_parser.set_defaults(func=cmd_generate)

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

    # load-postgres subcommand
    load_parser = subparsers.add_parser(
        "load-postgres",
        help="Load data from a manifest into PostgreSQL",
    )
    load_parser.add_argument(
        "--manifest",
        required=True,
        help="Path to the manifest file",
    )
    load_parser.add_argument(
        "--dsn",
        required=True,
        help="PostgreSQL connection string (DSN)",
    )
    load_parser.set_defaults(func=cmd_load_postgres)

    return parser


def main() -> int:
    """Main CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
