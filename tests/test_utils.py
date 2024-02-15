from collections import namedtuple
from datetime import date, datetime

import pandas as pd
import pytest

from utils import _check_for_new_results, ordinal_suffix


class TestUtils:
    def test_ordinal_suffix(self):
        assert all(ordinal_suffix(x) == "st" for x in [1, 21, 31, 1000001, 91])
        assert all(ordinal_suffix(x) != "st" for x in [11, 12, 13, 14, 15])
        assert all(ordinal_suffix(x) == "rd" for x in [3, 23, 93, 43])
        assert all(ordinal_suffix(x) == "nd" for x in [2, 22, 32, 42, 52])
        assert all(ordinal_suffix(x) == "th" for x in [12, 13, 112, 113])

    def test__check_for_new_results_month_one_new_data(self):
        year = 2024
        awarded = pd.DataFrame({"achievement_id": [3, 4],
                                "pax_id": ["U1", "U2"],
                                "date_awarded": [date(year, 1, 1), date(year, 2, 1)],
                                })
        awarded.date_awarded = pd.to_datetime(awarded.date_awarded)
        df = pd.DataFrame({"month": [1, 2],
                           "slack_user_id": ["U1", "U2"],
                           "home_region": ["f3chicago", "f3chicago"],
                           })
        Row = namedtuple("row", ["paxminer_schema", "slack_token"])
        row = Row("f3chicago", "fake_key")
        new_results = _check_for_new_results(row, year, 3, df, awarded)
        pd.testing.assert_frame_equal(new_results, pd.DataFrame({"month": [2,],
                                                                 "pax_id": ["U2",],
                                                                 "home_region": ["f3chicago",]},
                                                                 index=[1,]))

    def test__check_for_new_results_week_no_new_data(self):
        year = 2024
        awarded = pd.DataFrame({"achievement_id": [3, 3],
                                "pax_id": ["U1", "U2"],
                                "date_awarded": [date(year, 1, 1), date(year, 2, 1)],
                                })
        awarded.date_awarded = pd.to_datetime(awarded.date_awarded)
        df = pd.DataFrame({"week": [1, 5],
                           "slack_user_id": ["U1", "U2"],
                           "home_region": ["f3chicago", "f3chicago"],
                           })
        Row = namedtuple("row", ["paxminer_schema", "slack_token"])
        row = Row("f3chicago", "fake_key")
        new_results = _check_for_new_results(row, year, 3, df, awarded)
        assert new_results.empty

    def test__check_for_new_results_year_no_new_data(self):
        year = 2024
        awarded = pd.DataFrame({"achievement_id": [3, 3],
                                "pax_id": ["U1", "U2"],
                                "date_awarded": [date(year, 1, 1), date(year, 2, 1)],
                                })
        awarded.date_awarded = pd.to_datetime(awarded.date_awarded)
        df = pd.DataFrame({"year": [2024, 2024],
                           "slack_user_id": ["U1", "U2"],
                           "home_region": ["f3chicago", "f3chicago"],
                           })
        Row = namedtuple("row", ["paxminer_schema", "slack_token"])
        row = Row("f3chicago", "fake_key")
        new_results = _check_for_new_results(row, year, 3, df, awarded)
        assert new_results.empty

    def test__check_for_new_results_month_no_new_data(self):
        year = 2024
        awarded = pd.DataFrame({"achievement_id": [3, 3],
                                "pax_id": ["U1", "U2"],
                                "date_awarded": [date(year, 1, 1), date(year, 2, 1)],
                                })
        awarded.date_awarded = pd.to_datetime(awarded.date_awarded)
        df = pd.DataFrame({"month": [1, 2],
                           "slack_user_id": ["U1", "U2"],
                           "home_region": ["f3chicago", "f3chicago"],
                           })
        Row = namedtuple("row", ["paxminer_schema", "slack_token"])
        row = Row("f3chicago", "fake_key")
        new_results = _check_for_new_results(row, year, 3, df, awarded)
        assert new_results.empty

    def test__check_for_new_results_week_two_new_data(self):
        year = 2024
        awarded = pd.DataFrame({"achievement_id": [3, 3],
                                "pax_id": ["U1", "U2"],
                                "date_awarded": [date(year, 1, 1), date(year, 2, 1)],
                                })
        awarded.date_awarded = pd.to_datetime(awarded.date_awarded)
        df = pd.DataFrame({"week": [2, 3],
                           "slack_user_id": ["U1", "U2"],
                           "home_region": ["f3chicago", "f3chicago"],
                           })
        Row = namedtuple("row", ["paxminer_schema", "slack_token"])
        row = Row("f3chicago", "fake_key")
        new_results = _check_for_new_results(row, year, 3, df, awarded)
        assert new_results.shape[0] == 2

    def test__check_for_new_results_year_two_new_data(self):
        year = 2024
        awarded = pd.DataFrame(columns=["achievement_id", "pax_id", "date_awarded"])
        awarded.date_awarded = pd.to_datetime(awarded.date_awarded)
        df = pd.DataFrame({"year": [2024, 2024],
                           "slack_user_id": ["U1", "U2"],
                           "home_region": ["f3chicago", "f3chicago"],
                           })
        Row = namedtuple("row", ["paxminer_schema", "slack_token"])
        row = Row("f3chicago", "fake_key")
        new_results = _check_for_new_results(row, year, 3, df, awarded)
        assert new_results.shape[0] == 2
