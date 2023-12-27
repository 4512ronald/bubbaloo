import glob
import os
from typing import List
from dynaconf import Dynaconf


class Config:
    """
    Singleton class for managing application configuration.

    This class provides a centralized way of accessing and managing application settings
    using Dynaconf. It ensures that only one instance of configuration is created and
    used throughout the application. The configuration can be initialized with settings
    from a specified path or environment.

    Attributes:
        _instance: The single instance of the Config class.
        env (str): The environment name used for the configuration.
        path (str | List[str]): The file path(s) for configuration files.
        _conf (Dynaconf): The Dynaconf instance holding configuration data.
        _initialized (bool): A flag indicating whether the Config instance has been initialized.
    """

    _instance = None

    def __new__(cls, env: str, path: str | List[str] | None = None, **kwargs):
        """
        Creates a new Config instance if one doesn't already exist.

        Returns the existing instance if already created, ensuring the singleton pattern.

        Args:
            env (str): The environment name for the configuration.
            path (str | List[str] | None, optional): The path(s) to the configuration file(s).
            **kwargs: Additional keyword arguments for Dynaconf configuration.

        Returns:
            Config: The singleton Config instance.
        """
        if not cls._instance:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, env: str, path: str | List[str] | None = None, **kwargs):
        """
        Initializes the Config instance with the specified environment and path.

        Sets up the Dynaconf configuration instance with the given parameters. If the
        instance is already initialized, this method does nothing.

        Args:
            env (str): The environment name for the configuration.
            path (str | List[str] | None, optional): The path(s) to the configuration file(s).
            **kwargs: Additional keyword arguments for Dynaconf configuration.
        """
        if not self._initialized:
            self.env = env
            self.path: str | List[str] = self._get_path() if path is None else path
            self._conf: Dynaconf = Dynaconf(
                environments=True,
                env=self.env,
                includes=self.path,
                **kwargs
            )
            self._initialized = True

    @staticmethod
    def _get_path() -> List[str]:
        """
        Determines the configuration file path if not explicitly provided.

        Defaults to '*.toml' files in the working directory.

        Returns:
            str: The determined or provided file path for the configuration files.
        """
        path = os.getcwd()
        toml_files = glob.glob(f"{path}/*.toml")
        if toml_files:
            return toml_files
        else:
            raise FileNotFoundError("No TOML file found in the current directory.")

    def __getattr__(self, item):
        """
        Retrieves a configuration value as an attribute.

        Args:
            item: The configuration key to retrieve.

        Returns:
            The value of the specified configuration key.
        """
        return getattr(self._conf, item)

    def __getitem__(self, item):
        """
        Retrieves a configuration value using dictionary-like access.

        Args:
            item: The configuration key to retrieve.

        Returns:
            The value of the specified configuration key.
        """
        return self._conf[item]

    def get(self, item, default=None):
        """
        Retrieves a configuration value, returning a default value if the key is not found.

        Args:
            item: The configuration key to retrieve.
            default (optional): The default value to return if the key is not found.

        Returns:
            The value of the specified configuration key or the default value.
        """
        return self._conf.get(item, default)
