import base64
import re

import requests


def parse_github_url(file_url):

    match = re.match(r"https://github.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)", file_url)
    if not match:
        raise ValueError("Invalid GitHub URL format")

    repo_owner, repo_name, branch, file_path = match.groups()
    return repo_owner, repo_name, branch, file_path


def fetch_file_content(repo_owner, repo_name, file_path, branch, pat, run_mode):

    url_path_starter = 'https:'
    api_url = f"{url_path_starter}//api.github.com/repos/{repo_owner}/{repo_name}/contents/{file_path}?ref={branch}"

    headers = {"Authorization": f"token {pat}", "Accept": "application/vnd.github.v3+json"}

    verify = True if run_mode != 'local' else False

    response = requests.get(api_url, headers=headers, verify=verify)

    if response.status_code == 200:
        file_content = base64.b64decode(response.json()["content"]).decode("utf-8")
        return file_content
    else:
        error_message = response.json().get("message", "Unknown error")
        raise Exception(f"Error fetching file: {response.status_code} - {error_message}")
