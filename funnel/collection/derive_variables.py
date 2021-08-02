import warnings
from typing import Callable, List

import pydantic
from toolz import curry

from .registry import derived_variable_registry, query_dependent_operator_registry


@pydantic.dataclasses.dataclass
class DerivedVariable:
    """
    Support computation of variables that depend on multiple variables,
    i.e., "derived vars"
    """

    dependent_vars: List[str]
    func: Callable

    def __call__(self, ds, **kwargs):
        """call the function to compute derived var"""
        self._ensure_variables(ds)
        return self.func(ds, **kwargs)

    def _ensure_variables(self, ds):
        """ensure that required variables are present"""
        missing_var = set(self.dependent_vars) - set(ds.variables)
        if missing_var:
            raise ValueError(f'Variables missing: {missing_var}')


class QueryDependentOperator:
    """
    Support calling functions that depend on the values of the query.
    """

    def __init__(self, func, query_keys):
        self._callable = func
        self._query_keys = query_keys

    def __call__(self, ds, query_dict, **kwargs):
        """call function with query keys added to keyword args"""
        kwargs.update({k: query_dict[k] for k in self._query_keys})
        return self._callable(ds, **kwargs)


@curry
def register_derived_variable(func, varname, dependent_vars):
    """register a function for computing derived variables"""
    if varname in derived_variable_registry:
        warnings.warn(f'overwriting derived variable "{varname}" definition')

    derived_variable_registry[varname] = DerivedVariable(
        dependent_vars,
        func,
    )
    return func


@curry
def register_query_dependent_operator(func, query_keys):
    """register a function for computing derived variables"""
    func_hash = hash(func)
    if func_hash in query_dependent_operator_registry:
        warnings.warn(f'overwriting query dependent operator "{func.__name__}" definition')

    query_dependent_operator_registry[func_hash] = QueryDependentOperator(
        func,
        query_keys,
    )
    return func
