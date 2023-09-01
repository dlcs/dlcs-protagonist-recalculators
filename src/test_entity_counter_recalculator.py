import unittest
import os

import boto3
from botocore.exceptions import ParamValidationError

from unittest import mock
from moto import mock_cloudwatch, mock_ssm

import entity_counter_recalculator

from psycopg2.extras import RealDictCursor


class TestLambdaFunction(unittest.TestCase):

    @mock.patch("entity_counter_recalculator.ENABLE_CLOUDWATCH_INTEGRATION", False)
    @mock.patch("psycopg2.connect")
    @mock.patch("entity_counter_recalculator.CONNECTION_STRING", "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_lambda_handler_returns_mocked_values(self, mock_connect):
        expected = [['fake', 'row', 1], ['fake', 'row', 2]]

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        result = entity_counter_recalculator.begin_cleanup()

        self.assertEqual(result, {'customerImages': [['fake', 'row', 1],
                                                                  ['fake', 'row', 2]],
                                               'spaceImages': [['fake', 'row', 1], ['fake', 'row', 2]]})

        mock_con.cursor.asset_called_with(cursor_factory=RealDictCursor)

    @mock_cloudwatch
    @mock.patch("psycopg2.connect")
    @mock.patch("app.aws_factory")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME", "test1")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME", "test2")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_SPACE_DELETE_METRIC_NAME", "test3")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME", "test4")
    @mock.patch("entity_counter_recalculator.CONNECTION_STRING", "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_lambda_handler_updates_mocked_cloudfront_metrics(self, mock_connect, factory):
        expected = [['fake', 'row', 1], ['fake', 'row', 2]]

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        factory.get_aws_client = boto3.client("cloudwatch", region_name='eu-west-2')

        try:
            entity_counter_recalculator.begin_cleanup()
        except Exception:
            self.fail("myFunc() raised ExceptionType unexpectedly!")

        mock_con.cursor.asset_called_with(cursor_factory=RealDictCursor)

    @mock.patch("psycopg2.connect")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME", "test1")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME", "test2")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_SPACE_DELETE_METRIC_NAME", "test3")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME", "test4")
    @mock.patch("entity_counter_recalculator.CONNECTION_STRING", "")  # pragma: allowlist secret
    def test_lambda_handler_updates_raises_error_with_missing_env_variable(self, mock_connect):
        expected = []

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        with self.assertRaises(ParamValidationError):
            entity_counter_recalculator.begin_cleanup()


    @mock.patch("entity_counter_recalculator.CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME", "test1")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME", "test2")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_SPACE_DELETE_METRIC_NAME", "test3")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME", "test4")
    @mock.patch("entity_counter_recalculator.CONNECTION_STRING", "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    @mock_cloudwatch
    def test_set_cloudwatch_metrics_returns_0_for_all_metrics(self):
        aws_credentials()

        customer_images = {

        }

        space_images = {

        }

        records = {
            "customerImages": customer_images,
            "spaceImages": space_images
        }

        connection_info = {
            "database": "test"
        }

        cloudwatch = boto3.client("cloudwatch")

        metric_data = entity_counter_recalculator.set_cloudwatch_metrics(records, cloudwatch, connection_info)

        self.assertEqual(metric_data[0]["Value"], 0)
        self.assertEqual(metric_data[1]["Value"], 0)
        self.assertEqual(metric_data[2]["Value"], 0)
        self.assertEqual(metric_data[3]["Value"], 0)

    @mock.patch("entity_counter_recalculator.CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME", "test1")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME", "test2")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_SPACE_DELETE_METRIC_NAME", "test3")
    @mock.patch("entity_counter_recalculator.CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME", "test4")
    @mock.patch("entity_counter_recalculator.CONNECTION_STRING", "")
    @mock_cloudwatch
    def test_set_cloudwatch_metrics_returns_not_0_for_all_metrics(self):
        aws_credentials()
        customer_images = (
            {
                "delta": 10,
            },
            {
                "delta": None
            }
        )
        space_images = (
            {
                "delta": 10,
            },
            {
                "delta": None
            }
        )

        records = {
            "customerImages": customer_images,
            "spaceImages": space_images
        }

        connection_info = {
            "database": "test"
        }

        cloudwatch = boto3.client("cloudwatch")

        metric_data = entity_counter_recalculator.set_cloudwatch_metrics(records, cloudwatch, connection_info)

        self.assertEqual(metric_data[0]["Value"], 10)
        self.assertEqual(metric_data[1]["Value"], 1)
        self.assertEqual(metric_data[2]["Value"], 10)
        self.assertEqual(metric_data[3]["Value"], 1)


def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
