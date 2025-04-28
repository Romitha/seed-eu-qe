from typing import Any

import requests
import yaml
from bs4 import BeautifulSoup


def fetch_confluence_page_content(
    page_id: str, confluence_url: str, auth: tuple[str, str]
) -> str:
    """
    Fetch the content of a Confluence page

    Args:
        page_id (str): ID of the Confluence page
        confluence_url (str): Base URL of the Confluence instance
        auth (tuple[str, str]): Tuple of (username, api_token) for authentication

    Returns:
        str: Content of the Confluence page in HTML format

    Raises:
        requests.HTTPError: If the request fails
        ValueError: If the response structure is invalid

    Examples:
        >>> fetch_confluence_page_content(
        ...     "123456", "https://example.atlassian.net/wiki", ("user", "token")
        ... )
        "<html>...</html>"
    """
    if not all(isinstance(arg, str) for arg in (page_id, confluence_url)) or not isinstance(auth, tuple):
        raise TypeError("Invalid argument types: page_id and confluence_url "
                        "must be strings, auth must be a tuple")

    url = f"{confluence_url}/rest/api/content/{page_id}?expand=body.storage"
    response = requests.get(url, auth=auth)
    response.raise_for_status()

    content = response.json()
    if 'body' not in content or 'storage' not in content['body'] or 'value' not in content['body']['storage']:
        raise ValueError("Invalid response structure from Confluence API")

    return content['body']['storage']['value']


def extract_yaml_from_confluence_content(content: str) -> str:
    """
    Extract YAML content from Confluence page content

    Args:
        content (str): Raw HTML content fetched from Confluence

    Returns:
        str: Extracted YAML content as a string

    Raises:
        ValueError: If no YAML content is found

    Examples:
        >>> extract_yaml_from_confluence_content(
        ...     "<ac:structured-macro ac:name='code'>"
        ...     "<ac:plain-text-body><![CDATA[yaml_data]]>"
        ...     "</ac:plain-text-body></ac:structured-macro>"
        ... )
        "yaml_data"
    """
    if not isinstance(content, str):
        raise TypeError("content must be a string")

    soup = BeautifulSoup(content, 'html.parser')
    code_block = soup.find('ac:structured-macro', {'ac:name': 'code'})
    if code_block:
        cdata_block = code_block.find('ac:plain-text-body')
        if cdata_block:
            return cdata_block.text.strip()

    raise ValueError("No YAML content found in the Confluence page")


def convert_confluence_content_to_yaml(content: str) -> dict[str, Any]:
    """
    Convert Confluence content to a YAML dictionary

    Args:
        content (str): Content fetched from Confluence in HTML format

    Returns:
        dict[str, Any]: YAML content parsed into a dictionary

    Raises:
        ValueError: If YAML parsing fails

    Examples:
        >>> convert_confluence_content_to_yaml(
        ...     "<ac:structured-macro ac:name='code'>"
        ...     "<ac:plain-text-body><![CDATA[key: value]]>"
        ...     "</ac:plain-text-body></ac:structured-macro>"
        ... )
        {'key': 'value'}
    """
    yaml_content = extract_yaml_from_confluence_content(content)
    try:
        return yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        raise ValueError("Failed to parse YAML content") from e
