import unittest
import os

import boto3
from botocore.exceptions import ParamValidationError

from unittest import mock
from moto import mock_cloudwatch

import customer_storage_recalculator

from psycopg2.extras import RealDictCursor


class TestFunction(unittest.TestCase):

    @mock.patch("customer_storage_recalculator.ENABLE_CLOUDWATCH_INTEGRATION", False)
    @mock.patch("psycopg2.connect")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING",
                "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_handler_returns_mocked_values(self, mock_connect):
        expected = [['fake', 'row', 1], ['fake', 'row', 2]]

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.side_effect = [expected, []]

        result = customer_storage_recalculator.run_cleanup()

        self.assertEqual(result, {'spaceChanges': [['fake', 'row', 1], ['fake', 'row', 2]]})

        mock_con.cursor.assert_called_with(cursor_factory=RealDictCursor)

    @mock.patch("customer_storage_recalculator.ENABLE_CLOUDWATCH_INTEGRATION", False)
    @mock.patch("psycopg2.connect")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING",
                "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_handler_zeros_out_emptied_spaces(self, mock_connect):
        zeroed_changes = [
            {
                "customer": 1,
                "space": 2,
                "totalsizeofstoreimages": 100,
                "totalsizeinimage": 0,
                "totalsizedelta": 100,
                "numberofstoredimages": 5,
                "numberofimagesinimage": 0,
                "numberofimagesdelta": 5,
                "totalsizeofthumbnails": 50,
                "totalsizeofthumbnailsinimage": 0,
                "totalsizeofthumbnailsdelta": 50,
            }
        ]

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.side_effect = [[], zeroed_changes]

        result = customer_storage_recalculator.run_cleanup()

        self.assertEqual(result, {'spaceChanges': zeroed_changes})

    @mock.patch("customer_storage_recalculator.ENABLE_CLOUDWATCH_INTEGRATION", False)
    @mock.patch("psycopg2.connect")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING",
                "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_handler_returns_combined_space_and_zeroed_changes(self, mock_connect):
        space_changes = [{'customer': 1, 'space': 1, 'totalsizedelta': 50}]
        zeroed_changes = [{'customer': 1, 'space': 2, 'totalsizedelta': 100}]

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.side_effect = [space_changes, zeroed_changes]

        result = customer_storage_recalculator.run_cleanup()

        self.assertEqual(result, {'spaceChanges': space_changes + zeroed_changes})

    @mock.patch("customer_storage_recalculator.ENABLE_CLOUDWATCH_INTEGRATION", False)
    @mock.patch("customer_storage_recalculator.DRY_RUN", False)
    @mock.patch("psycopg2.connect")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING",
                "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_handler_commits_when_not_dry_run(self, mock_connect):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.side_effect = [[], []]

        customer_storage_recalculator.run_cleanup()

        mock_con.commit.assert_called_once()

    @mock.patch("customer_storage_recalculator.ENABLE_CLOUDWATCH_INTEGRATION", False)
    @mock.patch("customer_storage_recalculator.DRY_RUN", True)
    @mock.patch("psycopg2.connect")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING",
                "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_handler_does_not_commit_when_dry_run(self, mock_connect):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.side_effect = [[], []]

        customer_storage_recalculator.run_cleanup()

        mock_con.commit.assert_not_called()

    @mock_cloudwatch
    @mock.patch("app.aws_factory")
    @mock.patch("psycopg2.connect")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME", "test4")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME", "test5")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME", "test6")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING",
                "postgresql://user:pass@host:1234/postgres")  # pragma: allowlist secret
    def test_handler_updates_mocked_cloudfront_metrics(self, mock_connect, factory):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.side_effect = [[], []]

        factory.get_aws_client = boto3.client("cloudwatch", region_name='eu-west-2')

        try:
            customer_storage_recalculator.run_cleanup()
        except Exception:
            self.fail("myFunc() raised ExceptionType unexpectedly!")

        mock_con.cursor.assert_called_with(cursor_factory=RealDictCursor)

    @mock.patch("psycopg2.connect")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME", "test4")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME", "test5")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME", "test6")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING", "")
    def test_handler_updates_raises_error_with_missing_env_variable(self, mock_connect):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.side_effect = [[], []]

        with self.assertRaises(ParamValidationError):
            customer_storage_recalculator.run_cleanup()

    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME", "test4")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME", "test5")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME", "test6")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING", "")
    @mock_cloudwatch
    def test_set_cloudwatch_metrics_returns_0_for_all_metrics(self):
        aws_credentials()
        customer_level_changes = {

        }

        space_level_changes = {

        }

        records = {
            "customerChanges": customer_level_changes,
            "spaceChanges": space_level_changes
        }

        connection_info = {
            "database": "test"
        }

        cloudwatch = boto3.client("cloudwatch")

        metric_data = customer_storage_recalculator.set_cloudwatch_metrics(records, cloudwatch, connection_info)

        self.assertEqual(metric_data[0]["Value"], 0)
        self.assertEqual(metric_data[1]["Value"], 0)
        self.assertEqual(metric_data[2]["Value"], 0)

    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME", "test4")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME", "test5")
    @mock.patch("customer_storage_recalculator.CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME", "test6")
    @mock.patch("customer_storage_recalculator.CONNECTION_STRING", "")
    @mock_cloudwatch
    def test_set_cloudwatch_metrics_returns_20_for_all_metrics(self):
        aws_credentials()
        space_level_changes = (
            {
                "totalsizedelta": 10,
                "numberofimagesdelta": 10,
                "totalsizeofthumbnailsdelta": 10
            },
            {
                "totalsizedelta": 10,
                "numberofimagesdelta": 10,
                "totalsizeofthumbnailsdelta": 10
            }
        )

        records = {
            "spaceChanges": space_level_changes
        }

        connection_info = {
            "database": "test"
        }

        cloudwatch = boto3.client("cloudwatch")

        metric_data = customer_storage_recalculator.set_cloudwatch_metrics(records, cloudwatch, connection_info)

        self.assertEqual(metric_data[0]["Value"], 20)
        self.assertEqual(metric_data[1]["Value"], 20)
        self.assertEqual(metric_data[2]["Value"], 20)


def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
