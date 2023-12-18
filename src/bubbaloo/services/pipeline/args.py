import argparse


class ArgumentParser:
    """A class for managing command line argument parsing."""

    _parser = None

    @classmethod
    def get(cls) -> argparse.Namespace:
        """Retrieve the argument parser instance.

        Returns:
            argparse.Namespace: The parsed command line arguments.

        """
        if cls._parser is None:
            cls._parser = argparse.ArgumentParser(description="Parses command line arguments for the script")
            cls._parser.add_argument("-p", "--phase", required=False, help="phase of the architecture", type=str)
            cls._parser.add_argument(
                "-e",
                "--entity",
                required=False,
                help="Comma-separated list of values to be passed as input to the job",
                type=lambda entity: list(entity.split(',')),
            )
            cls._parser.add_argument("-v", "--env", required=True, help="variables environment", type=str)
            cls._parser.add_argument(
                "-p",
                "--conf_path",
                required=False,
                help="...",
                type=lambda path: list(path.split(',')),
                default=None
            )

        return cls._parser.parse_args()
