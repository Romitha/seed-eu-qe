import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from utils.common.aws_util import (get_glue_client, get_parameter_store_client,
                                   get_s3_client, get_secrets_manager_client,
                                   get_ses_client)


class TestAWSUtil(unittest.TestCase):

    @patch("utils.common.aws_util.boto3.Session")
    def test_get_s3_client_success(self, mock_session):
        """Test successful creation of S3 client"""
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        client = get_s3_client()
        mock_session.return_value.client.assert_called_once_with("s3")
        self.assertEqual(client, mock_client)

    @patch("utils.common.aws_util.boto3.Session", side_effect=NoCredentialsError)
    def test_get_s3_client_no_credentials(self, mock_session):
        """Test NoCredentialsError when credentials are missing"""
        with self.assertRaises(NoCredentialsError):
            get_s3_client()

    @patch("utils.common.aws_util.boto3.Session", side_effect=PartialCredentialsError(provider="aws", cred_var="access_key"))
    def test_get_s3_client_partial_credentials(self, mock_session):
        """Test PartialCredentialsError when credentials are incomplete"""
        with self.assertRaises(PartialCredentialsError):
            get_s3_client()

    @patch("utils.common.aws_util.boto3.client")
    def test_get_glue_client_success(self, mock_client):
        """Test successful creation of Glue client"""
        mock_client.return_value = MagicMock()
        client = get_glue_client()
        mock_client.assert_called_once_with("glue")
        self.assertIsNotNone(client)

    @patch("utils.common.aws_util.boto3.client", side_effect=NoCredentialsError)
    def test_get_glue_client_no_credentials(self, mock_client):
        """Test NoCredentialsError when credentials are missing"""
        with self.assertRaises(NoCredentialsError):
            get_glue_client()

    @patch("utils.common.aws_util.boto3.client", side_effect=PartialCredentialsError(provider="aws", cred_var="access_key"))
    def test_get_glue_client_partial_credentials(self, mock_client):
        """Test PartialCredentialsError when credentials are incomplete"""
        with self.assertRaises(PartialCredentialsError):
            get_glue_client()

    @patch("utils.common.aws_util.boto3.client")
    def test_get_secrets_manager_client_success(self, mock_client):
        """Test successful creation of Secrets Manager client"""
        mock_client.return_value = MagicMock()
        client = get_secrets_manager_client()
        mock_client.assert_called_once_with("secretsmanager")
        self.assertIsNotNone(client)

    @patch("utils.common.aws_util.boto3.client", side_effect=NoCredentialsError)
    def test_get_secrets_manager_client_no_credentials(self, mock_client):
        """Test NoCredentialsError when credentials are missing"""
        with self.assertRaises(NoCredentialsError):
            get_secrets_manager_client()

    @patch("utils.common.aws_util.boto3.client")
    def test_get_parameter_store_client_success(self, mock_client):
        """Test successful creation of Parameter Store client"""
        mock_client.return_value = MagicMock()
        client = get_parameter_store_client()
        mock_client.assert_called_once_with("ssm")
        self.assertIsNotNone(client)

    @patch("utils.common.aws_util.boto3.client", side_effect=NoCredentialsError)
    def test_get_parameter_store_client_no_credentials(self, mock_client):
        """Test NoCredentialsError when credentials are missing"""
        with self.assertRaises(NoCredentialsError):
            get_parameter_store_client()

    @patch("utils.common.aws_util.boto3.client")
    def test_get_ses_client_success(self, mock_client):
        """Test successful creation of SES client"""
        mock_client.return_value = MagicMock()
        client = get_ses_client()
        mock_client.assert_called_once_with("ses")
        self.assertIsNotNone(client)

    @patch("utils.common.aws_util.boto3.client", side_effect=NoCredentialsError)
    def test_get_ses_client_no_credentials(self, mock_client):
        """Test NoCredentialsError when credentials are missing"""
        with self.assertRaises(NoCredentialsError):
            get_ses_client()
