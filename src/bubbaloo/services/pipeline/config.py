from typing import List
from dynaconf import Dynaconf
import os


class Config:
    _instance = None

    def __new__(cls, env: str, path: str | List[str] | None = None, **kwargs):
        if not cls._instance:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, env: str, path: str | List[str] | None = None, **kwargs):
        if not self._initialized:
            self.env = env
            self.path: str | List[str] = self._get_path(path)
            self._conf: Dynaconf = Dynaconf(
                environments=True,
                env=self.env,
                includes=self.path,
                **kwargs
            )
            self._initialized = True

    @staticmethod
    def _get_path(path: str | List[str] | None = None) -> str:
        if path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
            path = f"{parent_dir}/*.toml"
        return path

    def __getattr__(self, item):
        return getattr(self._conf, item)

    def __getitem__(self, item):
        return self._conf[item]

    def get(self, item, default=None):
        return self._conf.get(item, default)
