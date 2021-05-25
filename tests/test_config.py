from funnel import settings
from funnel.config import Settings


def test_default_settings():
    assert Settings() == settings
