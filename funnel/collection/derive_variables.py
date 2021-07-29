import warnings
from typing import Callable

import pydantic
from funnel.collection.registry import derived_variable_registry, query_dependent_operator_registry
from toolz import curry


@pydantic.dataclasses.dataclass
class Derived_Variable(object):
    """
    Support computation of variables that depend on multiple variables,
    i.e., "derived vars"
    """

    dependent_vars: list
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


@curry
def register_derived_variable(func, varname, dependent_vars):
    """register a function for computing derived variables"""
    if varname in derived_variable_registry:
        warnings.warn(f'overwriting derived variable "{varname}" definition')

    derived_variable_registry[varname] = Derived_Variable(
        dependent_vars,
        func,
    )
    return func

@curry
def register_query_dependent_operator(func, query_keys):
    """register a function for computing derived variables"""
    func_hash = hash(func)
    if func_hash in query_dependent_operator_registry:
        warnings.warn(
            f'overwriting query dependent operator "{func.__name__}" definition'
        )

    query_dependent_operator_registry[func_hash] = query_dependent_op(
        func, query_keys,
    )    
    return func