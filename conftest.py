import pytest


def pytest_addoption(parser):
    """Used to pass database URL for DB docker container in integration test."""
    parser.addoption(
            "--database-url", action="store", default="sqlite://", help="URL to use for database tests. defaults to in-memory sqlite."
    )

@pytest.fixture
def database_url(request):
    return request.config.getoption("--database-url")
