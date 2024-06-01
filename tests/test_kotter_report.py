from datetime import date

import polars as pl

from kotter_report import build_kotter_report


def test_build_kotter_report():
    # Define test data
    df_posts = pl.DataFrame(
        [("email1", "user_id1", date(2022, 1, 1)), ("email2", "user_id2", date(2022, 1, 2))],
        schema=["email", "user_id", "date"]
    )
    df_qs = pl.DataFrame(
        [("email1", "user_id1", date(2022, 1, 1)), ("email2", "user_id2", date(2022, 1, 2))],
        schema=["email", "user_id", "date"]
    )
    df_noqs = pl.DataFrame(
        [("email1", "user_id1"), ("email2", "user_id2")],
        schema=["email", "user_id"]
    )
    siteq = "siteq1"

    # Call function with test data
    message = build_kotter_report(df_posts, df_qs, df_noqs, siteq)

    # Define expected message
    expected_message = (
        "Howdy, @siteq1! This is your weekly WeaselBot Site Q report. According to my records...\n\n"
        "The following PAX haven't posted in a bit. Now may be a good time to reach out to them when you get a minute. No OYO! :muscle:\n"
        "- <@email1> last posted 2022-01-01\n"
        "- <@email2> last posted 2022-01-02\n\n"
        "These guys haven't Q'd anywhere in a while (or at all!):\n"
        "- <@email1> hasn't been Q since 2022-01-01. That's 0 days!\n"
        "- <@email2> hasn't been Q since 2022-01-02. That's 0 days!\n"
        "- <@email1> (no Q yet!)\n"
        "- <@email2> (no Q yet!)"
    )

    # Assert that the returned message matches the expected message
    assert message == expected_message
