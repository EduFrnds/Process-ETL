from unittest.mock import MagicMock
from db.db_config import Connection


def test_connect_successful(mocker, db_config_mock):

    # GIVEN
    mock_connect = mocker.patch('psycopg2.connect', return_value=MagicMock())
    mock_connect.return_value.closed = 0

    # WHEN
    db = Connection()

    # THEN
    mock_connect.assert_called_once_with(**db.config['postgres'])
    assert db.connection is not None
    assert db.cursor is not None

    db.close()
    db.connection.closed = 1

    assert db.connection.closed == 1
