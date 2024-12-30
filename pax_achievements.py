#!/usr/bin/env /Users/jamessheldon/Library/Caches/pypoetry/virtualenvs/weaselbot-93dzw48B-py3.12/bin/python

import logging
from datetime import date
from typing import Tuple

import polars as pl
from sqlalchemy import MetaData, Selectable, Subquery, Table, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
from sqlalchemy.sql import and_, case, func, literal_column, or_, select, union_all

from utils import mysql_connection, send_to_slack


def home_region_sub_query(u: Table, a: Table, b: Table, ao: Table, date_range: int) -> Subquery[Tuple[str, int]]:
    """
    Abstract the subquery needed for length of time to look back for considering the home region. This is
    needed because there are many scenarios where a man could lapse in attending F3. Many different checks
    should be considered before defaulting to the maximium date range
    """
    return (
        select(u.c.email, func.count(a.c.user_id).label("attendance"))
        .select_from(
            u.join(a, a.c.user_id == u.c.user_id)
            .join(b, and_(a.c.q_user_id == b.c.q_user_id, a.c.ao_id == b.c.ao_id, a.c.date == b.c.bd_date))
            .join(ao, b.c.ao_id == ao.c.channel_id)
        )
        .where(func.datediff(func.curdate(), b.c.bd_date) < date_range)
        .group_by(u.c.email)
        .subquery()
    )


def build_home_regions(schemas: pl.DataFrame, metadata: MetaData, engine: Engine) -> Selectable[Tuple[str, str, str]]:
    """
    Constructs a SQL query to build home region attendance data for multiple schemas.
    This function filters out specific schemas and iterates over the remaining schemas to build
    SQL queries that calculate user attendance data for different date ranges (30, 60, 90, 120 days).
    The results are then combined into a single query using `union_all`.
    Args:
        schemas (pl.DataFrame): A DataFrame containing schema names.
        metadata (MetaData): SQLAlchemy MetaData object for reflecting tables.
        engine (Engine): SQLAlchemy Engine object for database connection.
    Returns:
        Selectable[Tuple[str, str, str]]: A SQLAlchemy selectable object representing the combined query.
    """

    queries = []
    schemas = schemas.filter(~pl.col("schema_name").is_in(["f3devcommunity", "f3development", "f3csra", "f3texarcana"]))
    for row in schemas.iter_rows():
        schema = row[0]
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            s1, s2, s3, s4 = (home_region_sub_query(u, a, b, ao, date_range) for date_range in (30, 60, 90, 120))

            sql = (
                select(
                    literal_column(f"'{schema}'").label("region"),
                    u.c.email,
                    case(
                        (s1.c.attendance.is_not(None), s1.c.attendance),
                        (s2.c.attendance.is_not(None), s2.c.attendance),
                        (s3.c.attendance.is_not(None), s3.c.attendance),
                        (s4.c.attendance.is_not(None), s4.c.attendance),
                        else_=func.count(a.c.user_id),
                    ).label("attendance"),
                )
                .select_from(
                    u.join(a, a.c.user_id == u.c.user_id)
                    .join(b, and_(a.c.q_user_id == b.c.q_user_id, a.c.ao_id == b.c.ao_id, a.c.date == b.c.bd_date))
                    .join(ao, b.c.ao_id == ao.c.channel_id)
                    .outerjoin(s1, u.c.email == s1.c.email)
                    .outerjoin(s2, u.c.email == s2.c.email)
                    .outerjoin(s3, u.c.email == s3.c.email)
                    .outerjoin(s4, u.c.email == s4.c.email)
                )
                .where(func.year(b.c.bd_date) == func.year(func.curdate()))
                .group_by(literal_column(f"'{schema}'").label("region"), u.c.email)
            )
            queries.append(sql)
        except SQLAlchemyError as e:
            logging.error(f"Schema {schema} error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in schema {schema}: {str(e)}")

    return union_all(*queries)


def nation_sql(
    schemas: pl.DataFrame, engine: Engine, metadata: MetaData
) -> Selectable[Tuple[str, str, str, str, str, str, int, str]]:
    """
    Generates a SQL query to retrieve user attendance and beatdown information from multiple schemas.
    Args:
        schemas (pl.DataFrame): A DataFrame containing schema names to be queried.
        engine (Engine): SQLAlchemy Engine object for database connection.
        metadata (MetaData): SQLAlchemy MetaData object for schema reflection.
    Returns:
        Selectable[Tuple[str, str, str, str, str, str, int, str]]: A union of SQL queries for each schema,
        selecting user email, user name, AO ID, AO name, beatdown date, Q flag, and backblast status.
    The function filters out specific schemas and iterates over the remaining schemas to construct
    SQL queries. It joins the 'users', 'bd_attendance', 'beatdowns', and 'aos' tables to gather
    relevant information. The queries are combined using a union_all operation.
    Raises:
        SQLAlchemyError: If there is an error with SQLAlchemy operations.
        Exception: For any other unexpected errors.
    """
    queries = []
    schemas = schemas.filter(~pl.col("schema_name").is_in(["f3devcommunity", "f3development", "f3csra", "f3texarcana"]))
    for row in schemas.iter_rows():
        schema = row[0]
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            sql = (
                select(
                    u.c.email,
                    u.c.user_name,
                    a.c.ao_id,
                    ao.c.ao.label("ao"),
                    b.c.bd_date.label("date"),
                    case((or_(a.c.user_id == b.c.q_user_id, a.c.user_id == b.c.coq_user_id), 1), else_=0).label(
                        "q_flag"
                    ),
                    b.c.backblast,
                )
                .select_from(
                    u.join(a, a.c.user_id == u.c.user_id)
                    .join(
                        b,
                        and_(
                            or_(a.c.q_user_id == b.c.q_user_id, a.c.q_user_id == b.c.coq_user_id),
                            a.c.ao_id == b.c.ao_id,
                            a.c.date == b.c.bd_date,
                        ),
                    )
                    .join(ao, b.c.ao_id == ao.c.channel_id)
                )
                .where(
                    func.year(b.c.bd_date) == func.year(func.curdate()),
                    b.c.bd_date <= date.today(),
                    u.c.email != "none",
                    u.c.user_name != "PAXminer",
                    b.c.q_user_id.is_not(None),
                )
            )
            queries.append(sql)
        except SQLAlchemyError as e:
            logging.error(f"Schema {schema} error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in schema {schema}: {str(e)}")

    return union_all(*queries)


def the_priest(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Filters and processes a DataFrame to identify users who have completed at least 25 Qsource lessons.

    Args:
        df (pl.DataFrame): The input DataFrame containing user data.
        bb_filter (pl.Expr): An expression to filter the DataFrame for a specific condition.
        ao_filter (pl.Expr): Another expression to filter the DataFrame for a different condition.

    Returns:
        pl.DataFrame: A DataFrame with columns 'year', 'email', 'region', and 'date_awarded' for users who have completed at least 25 lessons.
    """

    grouping = ["year", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) | (ao_filter))
        .group_by(pl.col(grouping))
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 25)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def the_monk(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Filters and processes a DataFrame to identify and award achievements based on specific criteria.
    Args:
        df (pl.DataFrame): The input DataFrame containing the data to be processed.
        bb_filter (pl.Expr): A filter expression to be applied to the DataFrame.
        ao_filter (pl.Expr): Another filter expression to be applied to the DataFrame.
    Returns:
        pl.DataFrame: A DataFrame containing the awarded achievements with columns:
            - "month": The month extracted from the "date" column.
            - "email": The email associated with the achievement.
            - "region": The region associated with the achievement.
            - "date_awarded": The maximum date from the filtered data, renamed from "date".
    """

    grouping = ["month", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((bb_filter) | (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 4)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def leader_of_men(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Filters and processes a DataFrame to identify leaders based on specific criteria.
    Args:
        df (pl.DataFrame): The input DataFrame containing the data.
        bb_filter (pl.Expr): A Polars expression used to filter the DataFrame.
        ao_filter (pl.Expr): Another Polars expression used to filter the DataFrame.
    Returns:
        pl.DataFrame: A DataFrame containing the filtered and processed data with columns:
            - "month": The month extracted from the "date" column.
            - "email": The email of the leader.
            - "region": The region of the leader.
            - "date_awarded": The maximum date from the "date" column for the group.
    """

    grouping = ["month", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 4)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def the_boss(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Processes the given DataFrame to filter and aggregate data based on specific criteria.
    Args:
        df (pl.DataFrame): The input DataFrame containing the data to be processed.
        bb_filter (pl.Expr): A filter expression to be applied to the DataFrame.
        ao_filter (pl.Expr): Another filter expression to be applied to the DataFrame.
    Returns:
        pl.DataFrame: A DataFrame containing the aggregated results with columns:
            - "month": The month extracted from the "date" column.
            - "email": The email address.
            - "region": The region.
            - "date_awarded": The maximum date from the "date" column for the filtered and grouped data.
    """

    grouping = ["month", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 6)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def hammer_not_nail(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Filters and processes a DataFrame to identify and award achievements based on specific criteria.
    Args:
        df (pl.DataFrame): The input DataFrame containing the data to be processed.
        bb_filter (pl.Expr): A Polars expression used to filter the DataFrame.
        ao_filter (pl.Expr): Another Polars expression used to filter the DataFrame.
    Returns:
        pl.DataFrame: A DataFrame containing the awarded achievements with columns:
            - "week": The week number extracted from the "date" column.
            - "email": The email associated with the achievement.
            - "region": The region associated with the achievement.
            - "date_awarded": The maximum date when the achievement was awarded.
    """

    grouping = ["week", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.week().alias("week"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 6)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def cadre(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Processes the given DataFrame to filter and group data based on specific criteria,
    and returns a DataFrame with the date awarded for each group.
    Args:
        df (pl.DataFrame): The input DataFrame containing the data to be processed.
        bb_filter (pl.Expr): A filter expression to be applied to the DataFrame.
        ao_filter (pl.Expr): Another filter expression to be applied to the DataFrame.
    Returns:
        pl.DataFrame: A DataFrame containing the grouped data with the date awarded for each group.
    """

    grouping = ["month", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").n_unique(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 7)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def el_presidente(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Filters and processes a DataFrame to identify users who meet specific criteria
    and awards them based on their activity.
    Parameters:
    df (pl.DataFrame): The input DataFrame containing user activity data.
    bb_filter (pl.Expr): A filter expression to apply to the DataFrame.
    ao_filter (pl.Expr): Another filter expression to apply to the DataFrame.
    Returns:
    pl.DataFrame: A DataFrame with columns 'year', 'email', 'region', and 'date_awarded'
                  for users who meet the criteria.
    """

    grouping = ["year", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 20)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def posts(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Processes the given DataFrame by applying filters, grouping, and aggregating data.
    Args:
        df (pl.DataFrame): The input DataFrame containing the data to be processed.
        bb_filter (pl.Expr): The filter expression to be applied to the DataFrame.
        ao_filter (pl.Expr): Another filter expression to be applied to the DataFrame.
    Returns:
        pl.DataFrame: A DataFrame with the aggregated results, grouped by year, email, and region.
    """

    grouping = ["year", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
    )
    return x


def six_pack(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Filters and processes a DataFrame to identify users who have achieved a "six pack"
    within a given week, based on specific filters.
    Args:
        df (pl.DataFrame): The input DataFrame containing user data.
        bb_filter (pl.Expr): A filter expression to apply to the DataFrame.
        ao_filter (pl.Expr): Another filter expression to apply to the DataFrame.
    Returns:
        pl.DataFrame: A DataFrame with columns 'week', 'email', 'region', and 'date_awarded',
                      containing users who have achieved a "six pack".
    """

    grouping = ["week", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.week().alias("week"))
        .filter((bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 6)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def hdtf(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """
    Processes the given DataFrame to filter, group, and aggregate data based on specified criteria.
    Args:
        df (pl.DataFrame): The input DataFrame containing the data to be processed.
        bb_filter (pl.Expr): The filter expression to be applied to the 'bb' column.
        ao_filter (pl.Expr): The filter expression to be applied to the 'ao' column.
    Returns:
        pl.DataFrame: A DataFrame containing the grouped and aggregated data with columns:
            - 'year': The year extracted from the 'date' column.
            - 'email': The email address.
            - 'region': The region.
            - 'date_awarded': The maximum date for the group.
    """

    grouping = ["year", "email", "region", "ao_id"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao").count(), pl.col("date").max())
        .filter(pl.col("ao") >= 50)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao", "date", "ao_id"])
    )
    return x


def load_to_database(schema: str, engine: Engine, metadata: MetaData, data_to_load: pl.DataFrame) -> None:
    """
    Load data into the database.
    This function attempts to load data into a table named "achievements_awarded" within the specified schema.
    If the table does not exist, it falls back to a table named "achievement_awarded". The data is loaded
    using SQLAlchemy's insert functionality.
    Args:
        schema (str): The schema in which the table resides.
        engine (Engine): The SQLAlchemy engine connected to the database.
        metadata (MetaData): The SQLAlchemy MetaData object.
        data_to_load (pl.DataFrame): The data to be loaded into the database, provided as a Polars DataFrame.
    Raises:
        NoSuchTableError: If neither "achievements_awarded" nor "achievement_awarded" tables are found.
    """

    try:
        aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=schema)
    except NoSuchTableError:
        aa = Table("achievement_awarded", metadata, autoload_with=engine, schema=schema)

    load_records = data_to_load.to_dicts()
    sql = insert(aa).values(load_records)
    with engine.begin() as cnxn:
        cnxn.execute(sql)


def main():
    """
    Main function to process and send achievement data to Slack channels for various regions.
    This function performs the following steps:
    1. Establishes a connection to the MySQL database.
    2. Retrieves schema names from the "regions" table.
    3. Builds SQL queries to fetch home regions and national beatdown data.
    4. Filters and processes the data to create various achievement dataframes.
    5. Iterates through each schema to:
        a. Retrieve AO table and Slack channel information.
        b. Fetch achievements awarded and achievements list data.
        c. Filter and join dataframes for the specific region.
        d. Send the processed data to Slack and load it into the database.
    6. Logs the progress and errors encountered during the process.
    7. Disposes of the database engine connection.
    Raises:
        NoSuchTableError: If a required table is not found in the schema.
    """

    year = date.today().year
    engine = mysql_connection()
    metadata = MetaData()
    uri = engine.url.render_as_string(hide_password=False).replace("+mysqlconnector", "")
    t = Table("regions", metadata, autoload_with=engine, schema="paxminer")
    sql = str(
        select(t.c.schema_name)
        .where(t.c.schema_name.like("f3%"))
        .compile(engine, compile_kwargs={"literal_binds": True})
    )
    schemas = pl.read_database_uri(query=sql, uri=uri)

    home_regions_sql = str(
        build_home_regions(schemas, metadata, engine).compile(engine, compile_kwargs={"literal_binds": True})
    )
    nation_query = str(nation_sql(schemas, engine, metadata).compile(engine, compile_kwargs={"literal_binds": True}))

    logging.info("Building home regions...")
    home_regions = pl.read_database_uri(query=home_regions_sql, uri=uri)
    logging.info("Building national beatdown data...")
    nation_df = pl.read_database_uri(query=nation_query, uri=uri).with_columns(
        pl.col("backblast").cast(pl.String()), pl.col("ao").cast(pl.String())
    )

    home_regions = home_regions.group_by("email").agg(pl.all().sort_by("attendance").last())
    nation_df = nation_df.join(home_regions.drop("attendance"), on="email")
    del home_regions

    # for QSource, we want to capture only QSource
    bb_filter = (
        pl.col("backblast").str.slice(0, 100).str.to_lowercase().str.contains(r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s")
    )
    ao_filter = pl.col("ao").str.to_lowercase().str.contains(r"q.{0,1}source")
    logging.info("Building national achievements dataframes...")

    dfs = []
    ############# Q Source ##############
    dfs.append(the_priest(nation_df, bb_filter, ao_filter))
    dfs.append(the_monk(nation_df, bb_filter, ao_filter))
    ############### END #################

    # For beatdowns, we want to exclude QSource and Ruck (blackops too? What is blackops?)
    bb_filter = ~pl.col("backblast").str.slice(0, 100).str.to_lowercase().str.contains(
        r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s"
    )
    ao_filter = ~pl.col("ao").str.to_lowercase().str.contains(r"q.{0,1}source|ruck")

    ############ ALL ELSE ###############
    dfs.append(leader_of_men(nation_df, bb_filter, ao_filter))
    dfs.append(the_boss(nation_df, bb_filter, ao_filter))
    dfs.append(hammer_not_nail(nation_df, bb_filter, ao_filter))
    dfs.append(cadre(nation_df, bb_filter, ao_filter))
    dfs.append(el_presidente(nation_df, bb_filter, ao_filter))

    s = posts(nation_df, bb_filter, ao_filter)
    for val in [25, 50, 100, 150, 200]:
        dfs.append(
            s.filter(pl.col("ao_id") >= val).with_columns(pl.col("date").alias("date_awarded")).drop(["ao_id", "date"])
        )

    dfs.append(six_pack(nation_df, bb_filter, ao_filter))
    dfs.append(hdtf(nation_df, bb_filter, ao_filter))

    logging.info("Parsing region info and sending to Slack...")
    for row in schemas.iter_rows():
        schema = row[0]
        if schema in ("f3devcommunity", "f3development", "f3csra", "f3texarcana", "f3yellowhammer"):
            continue
        try:
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)
        except NoSuchTableError:
            logging.error(f"No AO table found in in {schema}")
            continue

        with engine.begin() as cnxn:
            paxminer_log_channel = cnxn.execute(select(ao.c.channel_id).where(ao.c.ao == "paxminer_logs")).scalar()
            token = cnxn.execute(
                text(f"SELECT slack_token FROM weaselbot.regions WHERE paxminer_schema = '{schema}'")
            ).scalar()
            channel = cnxn.execute(
                text(f"SELECT achievement_channel FROM weaselbot.regions WHERE paxminer_schema = '{schema}'")
            ).scalar()
        if channel is None:
            logging.error(f"{schema} isn't signed up for Weaselbot achievements.")
            continue
        try:
            al = Table("achievements_list", metadata, autoload_with=engine, schema=schema)
        except NoSuchTableError:
            logging.error(f"{schema} isn't signed up for Weaselbot achievements.")
            continue
        try:
            aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=schema)
        except NoSuchTableError:
            aa = Table("achievement_awarded", metadata, autoload_with=engine, schema=schema)

        sql = (
            select(aa, al.c.code)
            .select_from(aa.join(al, aa.c.achievement_id == al.c.id))
            .where(func.year(aa.c.date_awarded) == func.year(func.curdate()))
        )

        awarded = pl.read_database_uri(str(sql.compile(engine, compile_kwargs={"literal_binds": True})), uri=uri)
        awards = pl.read_database_uri(f"SELECT * FROM {schema}.achievements_list", uri=uri)

        # we're pushing one schema at a time to Slack. Ensure all slack_id's are valid for that specific schema
        users = pl.read_database_uri(f"SELECT email, user_id as slack_user_id FROM {schema}.users", uri=uri)
        dfs_regional = []
        for df in dfs:
            dfs_regional.append(df.filter(pl.col("region") == schema).join(users, on="email").drop("email"))

        data_to_load = send_to_slack(schema, token, channel, year, awarded, awards, dfs_regional, paxminer_log_channel)
        if not data_to_load.is_empty():
            load_to_database(schema, engine, metadata, data_to_load)

        logging.info(f"Successfully loaded all records and sent all Slack messages for {schema}.")

    engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )
    main()
