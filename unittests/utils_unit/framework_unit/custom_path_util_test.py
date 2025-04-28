import os
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from utils.framework.custom_path_util import (
    find_project_root,
    get_custom_conf_root_path,
    get_framework_root_path,
    get_team_folder_path_with_key,
    get_teams_root_folder_path,
    get_team_sub_dir_path,
    get_cloud_data_checks_team_path,
    get_cloud_data_checks_sub_team_path,
)


class TestCustomPathUtil(unittest.TestCase):

    marker_file = None
    scratch_dir = None

    @classmethod
    def setUpClass(cls):
        cls.scratch_dir = (
            Path(__file__).resolve().parents[3] / "unittests" / "scratch_dir"
        )
        cls.scratch_dir.mkdir(parents=True, exist_ok=True)

        cls.marker_file = cls.scratch_dir / "poetry.lock"
        cls.marker_file.touch()

    @classmethod
    def tearDownClass(cls):
        # Clean up the scratch_dir after tests
        shutil.rmtree(cls.scratch_dir)

    def test_find_project_root(self):
        # Test finding the project root using a marker file
        root_path = find_project_root(self.marker_file)
        self.assertEqual(root_path, self.scratch_dir)

    def test_find_project_root_not_found(self):
        with self.assertRaises(FileNotFoundError):
            find_project_root(Path("/nonexistent/path"))

    def test_get_framework_root_path(self):
        # Test getting the framework root path
        root_path = get_framework_root_path()
        self.assertTrue(root_path.exists())
        self.assertTrue((root_path / "unittests").exists())

    def test_get_teams_root_folder_path(self):
        # Test getting the teams root folder path
        teams_path = get_teams_root_folder_path()
        expected = get_framework_root_path() / "custom_conf" / "teams"
        self.assertEqual(teams_path, expected)

    def test_get_custom_conf_root_path(self):
        # Test getting the custom configuration root folder path
        custom_conf_path = get_custom_conf_root_path()
        self.assertEqual(custom_conf_path, get_framework_root_path() / "custom_conf")

    def test_get_team_folder_path_with_key(self):
        # Update test based on single return value (only the path)
        base_path = Path("/base/path")
        team_key = "seed_intl_pgm"
        expected_path = base_path / "seed" / "intl" / "pgm"
        path = get_team_folder_path_with_key(base_path, team_key)
        self.assertEqual(path, expected_path)

    def test_get_team_sub_dir_path(self):
        base_path = Path("/teams/base")
        team_key = "marketing_insights_ai"
        expected = base_path / "marketing" / "insights"
        actual = get_team_sub_dir_path(base_path, team_key)
        self.assertEqual(actual, expected)

    def test_get_cloud_data_checks_team_path(self):
        team_key = "supply_chain_global"
        result = get_cloud_data_checks_team_path(team_key)
        self.assertEqual(result, "data-checks/supply/chain/global/")

    def test_get_cloud_data_checks_sub_team_path(self):
        team_key = "finance_risk_models"
        result = get_cloud_data_checks_sub_team_path(team_key)
        self.assertEqual(result, "data-checks/finance/risk/")
