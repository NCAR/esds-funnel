# Adapted from https://github.com/explosion/thinc
import sys
import typing

import catalogue

# Use typing_extensions for Python versions < 3.8
if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol


_DIn = typing.TypeVar('_DIn')


class Decorator(Protocol):
    def __call__(self, name: str) -> typing.Callable[[_DIn], _DIn]:
        ...


class registry:

    """Funnel's global registry entrypoint.

    This is used to register serializers and other components that are used by the funnel.
    """

    serializers: Decorator = catalogue.create('funnel', 'serializers', entry_points=True)
    metadata_store: Decorator = catalogue.create('funnel', 'metadata_store', entry_points=True)

    @classmethod
    def create(cls, registry_name: str, entry_points: bool = False) -> None:
        """Create a new custom registry."""
        if hasattr(cls, registry_name):
            raise ValueError(f"Registry '{registry_name}' already exists")
        reg: Decorator = catalogue.create('funnel', registry_name, entry_points=entry_points)
        setattr(cls, registry_name, reg)

    @classmethod
    def has(cls, registry_name: str, func_name: str) -> bool:
        """Check whether a function is available in a registry.

        Parameters
        ----------
        registry_name : str
            The name of the registry to check.
        func_name : str
            The name of the function to check.

        Returns
        -------
        bool
            Whether the function is available in the registry.

        """
        if not hasattr(cls, registry_name):
            return False
        reg = getattr(cls, registry_name)
        return func_name in reg

    @classmethod
    def get(cls, registry_name: str, func_name: str) -> typing.Callable:
        """Get a registered function from a given registry.

        Parameters
        ----------
        registry_name : str
            The name of the registry to get the function from.
        func_name : str
            The name of the function to get.

        Returns
        -------
        func : typing.Callable
            The function from the registry.
        """
        if not hasattr(cls, registry_name):
            raise ValueError(f"Unknown registry: '{registry_name}'")
        reg = getattr(cls, registry_name)
        func = reg.get(func_name)
        if func is None:
            raise ValueError(f"Could not find '{func_name}' in '{registry_name}'")
        return func
