"""NOTE: the pytest fixtures in this module should not be imported directly. they are registered as
pytest plugins in conftest.py. If this module is moved conftest.py pytest_plugins will also need to
be updated.
"""

from datetime import datetime, timedelta

import pytest

from tiktok_research_api_helper import utils


@pytest.mark.parametrize(
    ("end_date_offset", "is_caught_up"),
    [
        # end_date is exactly crawl_lag away from today, therefore we are caught up
        (0, True),
        # end_date is more than crawl_lag away from today, so not caught up
        (2, False),
        # end_date is more than crawl_lag away from today, so not caught up
        (1, False),
        # end_date is less than crawl_lag away from today, therefore caught up
        (-1, True),
    ],
)
def test_crawl_date_window_is_caught_up_to_today(end_date_offset, is_caught_up):
    crawl_lag = 3
    assert (
        utils.crawl_date_window_is_caught_up_to_today(
            utils.CrawlDateWindow(
                start_date=None,
                end_date=(datetime.now() - timedelta(days=end_date_offset + crawl_lag)),
            ),
            crawl_lag=crawl_lag,
        )
        == is_caught_up
    )
