import pydantic
import pytest

import funnel


def test_validate_error():
    with pytest.raises(pydantic.ValidationError):
        funnel.config.metadata_store = 5


def test_default_config():
    assert isinstance(funnel.config, pydantic.BaseSettings)
