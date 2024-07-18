from datetime import datetime, timedelta

import pytest

from tiktok_research_api_helper import utils


@pytest.mark.parametrize(
    ("crawl_lag", "days_behind_today", "expected"),
    [
        # end_date is exactly crawl_lag away from today, therefore we are caught up
        (3, 3, False),
        (4, 4, False),
        # end_date is less than crawl_lag away from today, therefore caught up
        (3, 2, False),
        (4, 3, False),
        # end_date is more than crawl_lag away from today, so not caught up
        (3, 5, True),
        (4, 6, True),
        # end_date is more than crawl_lag away from today, so not caught up
        (3, 4, True),
        (4, 5, True),
    ],
)
def test_crawl_date_window_is_behind_today(crawl_lag, days_behind_today, expected):
    crawl_date_window = utils.CrawlDateWindow(
        start_date=None,
        end_date=(datetime.now() - timedelta(days=days_behind_today)),
    )
    assert (
        utils.crawl_date_window_is_behind_today(
            crawl_date_window,
            crawl_lag=crawl_lag,
        )
        == expected
    )
