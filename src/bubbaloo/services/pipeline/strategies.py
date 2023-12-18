import importlib
import pkgutil
from typing import Iterable


class LoadStrategies:

    def __init__(self, attr_name: str, package_path: str, path: Iterable[str] | None = None, prefix: str = ""):
        self.path = path
        self.prefix = prefix
        self.package_path = package_path
        self.attr_name = attr_name
        self._strategies = self._load_strategies()

    def _load_strategies(self):
        for _, name, _ in pkgutil.iter_modules(self.path):
            try:
                module = importlib.import_module(f'{self.package_path}.{name}')
                if hasattr(module, self.attr_name):
                    self._strategies[name] = getattr(module, self.attr_name)
            except Exception as e:
                raise ModuleNotFoundError(f"Cannot import class {name}") from e

    def get_strategy(self, entity: str):
        if entity not in self._strategies:
            raise ValueError(f"No strategy found for entity: {entity}")
        return self._strategies[entity]
