"""CLI entrypoint for text2sql-synth."""

import argparse
import sys


def cmd_generate(args: argparse.Namespace) -> int:
    """Generate synthetic data from a configuration file."""
    print(f"generate: config={args.config}, out={args.out}")
    print("NOT IMPLEMENTED")
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
        help="Generate synthetic data from a configuration file",
    )
    gen_parser.add_argument(
        "--config",
        required=True,
        help="Path to the generation configuration file",
    )
    gen_parser.add_argument(
        "--out",
        required=True,
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
