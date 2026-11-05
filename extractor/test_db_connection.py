from types import SimpleNamespace
from unittest.mock import Mock, patch
import os
import unittest

from db_connection import connect_pg18


class RoutingTests(unittest.TestCase):
    def test_default_socket_and_explicit_dsn_precedence(self):
        conn = Mock(info=SimpleNamespace(dbname="crossref", user="languageingenetics", port=5433), server_version=180006)
        with patch.dict(os.environ, {}, clear=True), patch("db_connection.psycopg2.connect", return_value=conn) as connect:
            self.assertIs(connect_pg18(), conn)
            connect.assert_called_once_with(dbname="crossref", host="/var/run/postgresql", port="5433")
            conn.cursor.assert_not_called()
        with patch.dict(os.environ, {"PGPORT":"5432"}, clear=True), patch("db_connection.psycopg2.connect", return_value=conn) as connect:
            connect_pg18("host=raksasa port=5433 dbname=crossref user=languageingenetics")
            self.assertEqual(connect.call_args.kwargs["port"], "5433")

    def test_wrong_route_closes_before_application_sql(self):
        for database,port,major in [("crossref",5432,16),("crossref",5432,18),("crossref",5433,16),("other",5433,18)]:
            conn=Mock(info=SimpleNamespace(dbname=database,user="languageingenetics",port=port),server_version=major*10000)
            with self.subTest(database=database,port=port,major=major), patch.dict(os.environ,{},clear=True), patch("db_connection.psycopg2.connect",return_value=conn):
                with self.assertRaises(RuntimeError):connect_pg18()
                conn.close.assert_called_once();conn.cursor.assert_not_called()

    def test_isolated_database_requires_explicit_expectation(self):
        conn=Mock(info=SimpleNamespace(dbname="crossref_derived_verify_example",user="languageingenetics",port=5433),server_version=180006)
        with patch.dict(os.environ,{"LIG_EXPECTED_DATABASE":"crossref_derived_verify_example"},clear=True), patch("db_connection.psycopg2.connect",return_value=conn):
            self.assertIs(connect_pg18(dbname="crossref_derived_verify_example",host=None,user=None),conn)


if __name__ == "__main__":unittest.main()
