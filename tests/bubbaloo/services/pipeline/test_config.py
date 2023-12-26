from bubbaloo.services.pipeline.config import Config


class TestConfig:
    """
    A test class for verifying the functionality of the Config class.

    This class tests various aspects of the Config class, including its singleton behavior, initialization process, and
    methods for accessing configuration values.
    """

    def test_singleton(self, config, env, temp_config_file):
        """
        Test the singleton behavior of the Config class.

        This test verifies that only one instance of the Config class is created for a given environment and
        configuration file path.

        Args:
            config (Config): The existing Config instance.
            env (str): The environment name used for initializing Config.
            temp_config_file (str): The path to a temporary configuration file.
        """
        new_config = Config(env, temp_config_file)
        assert new_config is config

    def test_initialization(self, config, env, temp_config_file):
        """
        Test the initialization of the Config class.

        This test verifies that the Config class is correctly initialized with the given environment and configuration
        file path.

        Args:
            config (Config): The existing Config instance.
            env (str): The environment name used for initializing Config.
            temp_config_file (str): The path to a temporary configuration file.
        """
        assert config.env == env
        assert config.path == temp_config_file

    def test_getattr(self, config, raw_temp_dir):
        """
        Test the attribute access functionality of the Config class.

        This test checks that configuration values can be accessed as attributes of the Config instance.

        Args:
            config (Config): The Config instance.
            raw_temp_dir (Path): The path to a temporary directory used in the configuration.
        """
        assert config.raw_path == str(raw_temp_dir)

    def test_getitem(self, config, raw_temp_dir):
        """
        Test the item access functionality of the Config class.

        This test verifies that configuration values can be accessed using the item access syntax.

        Args:
            config (Config): The Config instance.
            raw_temp_dir (Path): The path to a temporary directory used in the configuration.
        """
        assert config["raw_path"] == str(raw_temp_dir)

    def test_get(self, config, raw_temp_dir):
        """
        Test the 'get' method of the Config class.

        This test checks that the 'get' method correctly retrieves configuration values, and returns a default value
        when a key is not present.

        Args:
            config (Config): The Config instance.
            raw_temp_dir (Path): The path to a temporary directory used in the configuration.
        """
        assert config.get("raw_path") == str(raw_temp_dir)
        assert config.get("nonexistent_key", "default") == "default"
