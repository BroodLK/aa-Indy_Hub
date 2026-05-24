"""Tests for Indy Hub periodic task synchronization hooks."""

# Standard Library
import sys
import types
from importlib import import_module
from unittest.mock import Mock, patch

# Django
from django.test import SimpleTestCase

from indy_hub.apps import IndyHubConfig


class IndyHubAppConfigTests(SimpleTestCase):
    def test_ready_syncs_periodic_tasks_on_startup(self):
        config = IndyHubConfig("indy_hub", import_module("indy_hub"))
        fake_tasks = types.ModuleType("indy_hub.tasks")
        fake_tasks.ensure_task_submodules_imported = Mock()

        with (
            patch.dict(sys.modules, {"indy_hub.tasks": fake_tasks}),
            patch("indy_hub.apps.import_module"),
            patch("indy_hub.apps.post_migrate.connect") as mock_connect,
            patch.object(config, "_setup_periodic_tasks") as mock_setup_periodic_tasks,
        ):
            config.ready()

        fake_tasks.ensure_task_submodules_imported.assert_called_once_with()
        mock_connect.assert_called_once()
        mock_setup_periodic_tasks.assert_called_once()
