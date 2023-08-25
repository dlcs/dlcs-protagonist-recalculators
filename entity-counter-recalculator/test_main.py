import unittest

import boto3
from botocore.exceptions import ParamValidationError

from unittest import mock
from moto import mock_cloudwatch, mock_ssm

import main

from psycopg2.extras import RealDictCursor


class TestLambdaFunction(unittest.TestCase):

    @mock.patch("main.ENABLE_CLOUDWATCH_INTEGRATION", False)
    @mock.patch("psycopg2.connect")
    @mock.patch("main.CONNECTION_STRING", "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_lambda_handler_returns_mocked_values(self, mock_connect):
        expected = [['fake', 'row', 1], ['fake', 'row', 2]]

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        result = main.begin_cleanup()

        self.assertEqual(result, {'customerImages': [['fake', 'row', 1],
                                                                  ['fake', 'row', 2]],
                                               'spaceImages': [['fake', 'row', 1], ['fake', 'row', 2]]})

        mock_con.cursor.asset_called_with(cursor_factory=RealDictCursor)

    @mock_cloudwatch
    @mock.patch("psycopg2.connect")
    @mock.patch("app.aws_factory")
    @mock.patch("main.CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME", "test1")
    @mock.patch("main.CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME", "test2")
    @mock.patch("main.CLOUDWATCH_SPACE_DELETE_METRIC_NAME", "test3")
    @mock.patch("main.CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME", "test4")
    @mock.patch("main.CONNECTION_STRING", "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_lambda_handler_updates_mocked_cloudfront_metrics(self, mock_connect, factory):
        expected = [['fake', 'row', 1], ['fake', 'row', 2]]

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        factory.get_aws_client = boto3.client("cloudwatch", region_name='eu-west-2')

        try:
            main.begin_cleanup()
        except Exception:
            self.fail("myFunc() raised ExceptionType unexpectedly!")

        mock_con.cursor.asset_called_with(cursor_factory=RealDictCursor)

   # @mock_cloudwatch
    @mock.patch("psycopg2.connect")
    @mock.patch("main.CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME", "test1")
    @mock.patch("main.CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME", "test2")
    @mock.patch("main.CLOUDWATCH_SPACE_DELETE_METRIC_NAME", "test3")
    @mock.patch("main.CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME", "test4")
    @mock.patch("main.CONNECTION_STRING", "")
    def test_lambda_handler_updates_raises_error_with_missing_env_variable(self, mock_connect):
        expected = []

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        with self.assertRaises(ParamValidationError):
            main.begin_cleanup()

    # @mock_cloudwatch
    # @mock_ssm
    # @mock.patch("psycopg2.connect")
    # @mock.patch("app.aws_factory")
    # @mock.patch("app.aws_factory.get_aws_client")
    # @mock.patch("main.CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME", "test1")
    # @mock.patch("main.CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME", "test2")
    # @mock.patch("main.CLOUDWATCH_SPACE_DELETE_METRIC_NAME", "test3")
    # @mock.patch("main.CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME", "test4")
    # def test_lambda_handler_updates_from_ssm_connection_string(self, mock_connect, mock_factory, mock_client):
    #     expected = [['fake', 'row', 1], ['fake', 'row', 2]]
    #
    #     mock_con = mock_connect.return_value
    #     mock_cur = mock_con.cursor.return_value
    #     mock_cur.fetchall.return_value = expected
    #
    #     # aws_factory.get_aws_client().get_parameter()
    #    # test.return_value.get_parameter.return_value = "postgresql://user:pass@host:1234/postgres"
    #    # mock_factory.get_aws_client = MagicMock()
    #     #mock_factory = MagicMock()
    #    # mock_factory.return_value.get_aws_client = MagicMock()
    #     mock_client.side_effect = "postgresql://user:pass@host:1234/postgres"
    #
    #     try:
    #         main.lambda_handler(event="{}", context=None)
    #     except Exception:
    #         self.fail("myFunc() raised ExceptionType unexpectedly!")
    #
    #     mock_con.cursor.asset_called_with(cursor_factory=RealDictCursor)

