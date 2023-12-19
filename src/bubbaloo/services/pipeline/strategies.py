import importlib
import pkgutil
from typing import Iterable, Dict, Type


class LoadStrategies:
    """Manages the loading of strategies from a given package.

    Attributes:
        path (Iterable[str] | None): The file system paths to search for modules, if None defaults to package_path.
        prefix (str): A prefix to add to the name of the found modules.
        package_path (str): The package path from where to load the modules.
        attr_name (str): The attribute name to look for in the modules.
        _strategies (Dict[str, Type]): A dictionary mapping strategy names to their respective classes.
    """

    def __init__(self, attr_name: str, package_path: str, path: Iterable[str] | None = None, prefix: str = ""):
        self.path = path
        self.prefix = prefix
        self.package_path = package_path
        self.attr_name = attr_name
        self._strategies: Dict[str, Type] = {}
        self._load_strategies()

    def _load_strategies(self):
        """Loads strategies from modules found in the specified package path."""
        for _, name, _ in pkgutil.iter_modules(self.path):
            try:
                module = importlib.import_module(f'{self.package_path}.{name}')
                if hasattr(module, self.attr_name):
                    self._strategies[name] = getattr(module, self.attr_name)
            except Exception as e:
                raise ModuleNotFoundError(f"Cannot import class {name}") from e

    def get_strategy(self, entity: str) -> Type:
        """Retrieves a strategy class for the given entity name.

        Args:
            entity (str): The name of the entity for which to get the strategy.

        Returns:
            Type: The strategy class associated with the given entity.

        Raises:
            ValueError: If no strategy is found for the specified entity.
        """
        if entity not in self._strategies:
            raise ValueError(f"No strategy found for entity: {entity}")
        return self._strategies[entity]
