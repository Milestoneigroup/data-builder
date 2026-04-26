"""CLI entrypoint (intentionally minimal until workflows are wired)."""

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="data-builder",
        description="Milestone data-builder CLI",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print version and exit",
    )
    args = parser.parse_args()
    if args.version:
        from data_builder import __version__

        print(__version__)
        return
    parser.print_help()


if __name__ == "__main__":
    main()
