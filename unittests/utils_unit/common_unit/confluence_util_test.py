import unittest
from unittest.mock import MagicMock, patch

import requests

from utils.common.confluence_util import (convert_confluence_content_to_yaml,
                                          extract_yaml_from_confluence_content,
                                          fetch_confluence_page_content)


class TestConfluenceUtils(unittest.TestCase):

    @patch("requests.get")
    def test_fetch_confluence_page_content(self, mock_get):
        """Test fetching Confluence page content"""

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "body": {
                "storage": {
                    "value": "<html>mock content</html>"
                }
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_confluence_page_content("12345", "https://example.com", ("user", "token"))
        self.assertEqual(result, "<html>mock content</html>")

        # Test invalid response format
        mock_response.json.return_value = {"invalid": "data"}
        with self.assertRaises(ValueError):
            fetch_confluence_page_content("12345", "https://example.com", ("user", "token"))

        # Test request failure
        mock_response.raise_for_status.side_effect = requests.HTTPError("Error")
        with self.assertRaises(requests.HTTPError):
            fetch_confluence_page_content("12345", "https://example.com", ("user", "token"))

        # Test invalid argument types
        with self.assertRaises(TypeError):
            fetch_confluence_page_content(12345, "https://example.com", ("user", "token"))

        with self.assertRaises(TypeError):
            fetch_confluence_page_content("12345", ["https://example.com"], ("user", "token"))

        with self.assertRaises(TypeError):
            fetch_confluence_page_content("12345", "https://example.com", "invalid_auth")

    def test_extract_yaml_from_confluence_content(self):
        """Test extracting YAML from Confluence content"""

        valid_content = """
        <ac:structured-macro ac:name='code'>
            <ac:plain-text-body><![CDATA[key: value]]></ac:plain-text-body>
        </ac:structured-macro>
        """
        self.assertEqual(extract_yaml_from_confluence_content(valid_content), "key: value")

        # Test missing code block
        invalid_content = "<html>No YAML here</html>"
        with self.assertRaises(ValueError):
            extract_yaml_from_confluence_content(invalid_content)

        # Test invalid argument type
        with self.assertRaises(TypeError):
            extract_yaml_from_confluence_content(123)

    def test_convert_confluence_content_to_yaml(self):
        """Test converting Confluence content to YAML dictionary"""

        valid_content = """
        <ac:structured-macro ac:name='code'>
            <ac:plain-text-body><![CDATA[key: value]]></ac:plain-text-body>
        </ac:structured-macro>
        """
        expected_output = {"key": "value"}
        self.assertEqual(convert_confluence_content_to_yaml(valid_content), expected_output)

        # Test invalid YAML content
        invalid_content = """
        <ac:structured-macro ac:name='code'>
            <ac:plain-text-body><![CDATA[key: : value]]></ac:plain-text-body>
        </ac:structured-macro>
        """
        with self.assertRaises(ValueError):
            convert_confluence_content_to_yaml(invalid_content)
