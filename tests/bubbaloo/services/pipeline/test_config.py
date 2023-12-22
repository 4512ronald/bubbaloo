from bubbaloo.services.pipeline.config import Config


class TestConfig:

    def test_singleton(self, config, env, temp_config_file):
        new_config = Config(env, temp_config_file)
        assert new_config is config

    def test_initialization(self, config, env, temp_config_file):
        assert config.env == env
        assert config.path == temp_config_file

    def test_getattr(self, config, raw_temp_dir):
        assert config.raw_path == str(raw_temp_dir)

    def test_getitem(self, config, raw_temp_dir):
        assert config["raw_path"] == str(raw_temp_dir)

    def test_get(self, config, raw_temp_dir):
        assert config.get("raw_path") == str(raw_temp_dir)
        assert config.get("nonexistent_key", "default") == "default"
