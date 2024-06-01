import pytest
from sqlalchemy import select
from sqlalchemy.engine import Engine

from achievement_tables import engine, metadata


@pytest.fixture(scope="module")
def connection():
    with engine.begin() as cnxn:
        yield cnxn


class TestAchievementTables:
    def test_achievements_list_table_exists(self, connection: Engine):
        assert connection.dialect.has_table(connection, "achievements_list")

    def test_achievements_awarded_table_exists(self, connection: Engine):
        assert connection.dialect.has_table(connection, "achievements_awarded")

    def test_inserted_achievements(self, connection: Engine):
        achievements_list = metadata.tables["f3development.achievements_list"]
        select_query = select(achievements_list).where(achievements_list.c.name == "The Priest")
        result = connection.execute(select_query).fetchone()
        assert result is not None
        assert result.name == "The Priest"
        assert result.description == "Post for 25 Qsource lessons"
        assert result.verb == "posting for 25 Qsource lessons"
        assert result.code == "the_priest"

    def test_users_table_exists(self, connection: Engine):
        assert connection.dialect.has_table(connection, "users")

    def test_achievements_view_exists(self, connection: Engine):
        assert connection.dialect.has_table(connection, "achievements_view")

    def test_achievements_view_data(self, connection: Engine):
        achievements_view = metadata.tables["f3development.achievements_view"]
        select_query = select(achievements_view).where(achievements_view.c.pax == "JohnDoe")
        result = connection.execute(select_query).fetchone()
        assert result is not None
        assert result.pax == "JohnDoe"
        assert result.name == "The Priest"
        assert result.description == "Post for 25 Qsource lessons"
        assert result.date_awarded is not None
