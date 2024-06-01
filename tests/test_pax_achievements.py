from unittest.mock import MagicMock, call, patch

from sqlalchemy.exc import NoSuchTableError

from pax_achievements import main


@patch('pax_achievements.mysql_connection')
def test_main_database_connection(mock_mysql_connection):
    mock_mysql_connection.return_value = MagicMock()
    main()
    mock_mysql_connection.assert_called_once()

@patch('pax_achievements.pl.read_database_uri')
@patch('pax_achievements.mysql_connection')
def test_main_database_read(mock_mysql_connection, mock_read_database_uri):
    mock_mysql_connection.return_value = MagicMock()
    mock_read_database_uri.return_value = MagicMock()
    main()
    assert call(str(anything), anything) in mock_read_database_uri.call_args_list

@patch('pax_achievements.load_to_database')
@patch('pax_achievements.mysql_connection')
def test_main_database_write(mock_mysql_connection, mock_load_to_database):
    mock_mysql_connection.return_value = MagicMock()
    mock_load_to_database.return_value = MagicMock()
    main()
    assert call(anything, anything, anything, anything) in mock_load_to_database.call_args_list

@patch('pax_achievements.send_to_slack')
@patch('pax_achievements.mysql_connection')
def test_main_slack_message_sending(mock_mysql_connection, mock_send_to_slack):
    mock_mysql_connection.return_value = MagicMock()
    mock_send_to_slack.return_value = MagicMock()
    main()
    assert call(anything, anything, anything, anything, anything, anything, anything, anything) in mock_send_to_slack.call_args_list

@patch('pax_achievements.Table')
@patch('pax_achievements.mysql_connection')
def test_main_exception_handling(mock_mysql_connection, mock_table):
    mock_mysql_connection.return_value = MagicMock()
    mock_table.side_effect = NoSuchTableError
    with patch('pax_achievements.logging.error') as mock_log:
        main()
        assert call(f"No AO table found in in {anything}") in mock_log.call_args_list