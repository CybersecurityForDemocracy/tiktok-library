import pytest

# Register tests/test_utils.py module as a pytest plugin so that tests can use fixtures from that
# module without importing them (pytest recommends against importing them, and linters will flag the
# import as unused and the fixture usage in test method args as redefining the var; https://github.com/astral-sh/ruff/issues/4046)
pytest_plugins="tests.test_utils"

def pytest_addoption(parser):
    """Used to pass database URL for DB docker container in integration test."""
    parser.addoption(
        "--database-url",
        action="store",
        default=None,
        help="URL to use for database tests. defaults to in-memory sqlite.",
    )


@pytest.fixture
def database_url_command_line_arg(request):
    return request.config.getoption("--database-url")

