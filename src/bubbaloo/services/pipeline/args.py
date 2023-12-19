import argparse


class ArgumentParser:
    """
    A class for parsing command line arguments for a script.

    This class uses the argparse module to define and parse command line arguments.
    It is designed to encapsulate argument parsing logic, providing a convenient
    interface to access parsed arguments.

    Attributes:
        _parser (argparse.ArgumentParser): The parser for command line arguments.
        _args (argparse.Namespace): Parsed command line arguments.
    """

    def __init__(self):
        """
        Initializes the ArgumentParser instance.

        Sets up the argument parser with a description and defines the arguments
        that the script expects. The arguments are then parsed and stored.
        """
        self._parser = argparse.ArgumentParser(description="Parses command line arguments for the script")
        self._define_arguments()
        self._args = self._parse_arguments()

    @property
    def args(self):
        """
        Provides access to the parsed command line arguments.

        Returns:
            argparse.Namespace: The parsed command line arguments.
        """
        return self._args

    def _define_arguments(self) -> None:
        """
        Defines the command line arguments to be parsed.

        Sets up various arguments with their respective flags, help descriptions,
        and types. Some arguments are optional, while others are required.
        """
        self._parser.add_argument(
            "-p", "--phase",
            required=False,
            help="Specify the phase of the architecture. This is an optional argument.",
            type=str
        )
        self._parser.add_argument(
            "-e", "--entity",
            required=False,
            help="Provide a comma-separated list of entities. Each entity is a value to be passed as input to the job.",
            type=lambda entity: list(entity.split(','))
        )
        self._parser.add_argument(
            "-v", "--env",
            required=True,
            help="Set the environment variables. This is a required argument.",
            type=str
        )
        self._parser.add_argument(
            "-c", "--conf_path",
            required=False,
            help="Provide a comma-separated list of configuration paths. This is an optional argument.",
            type=lambda path: list(path.split(',')),
            default=None
        )

    def _parse_arguments(self) -> argparse.Namespace:
        """
        Parses the command line arguments.

        Uses the argparse module to parse arguments passed to the script.

        Returns:
            argparse.Namespace: The parsed command line arguments.
        """
        return self._parser.parse_args()
