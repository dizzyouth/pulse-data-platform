"""Policy contracts for CI; actionlint validates the full Actions schema locally."""

from __future__ import annotations

import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CIConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        # The workflow quotes "on" so PyYAML's YAML 1.1 boolean resolver does
        # not turn the Actions event key into True.
        cls.workflow = yaml.safe_load(cls.text)
        cls.job = cls.workflow["jobs"]["quality"]
        cls.steps = cls.job["steps"]
        cls.commands = "\n".join(step.get("run", "") for step in cls.steps)

    def test_triggers_and_obsolete_run_cancellation(self) -> None:
        events = self.workflow["on"]
        self.assertEqual(set(events), {"pull_request", "push"})
        self.assertEqual(events["push"]["branches"], ["main"])
        self.assertIsNone(events["pull_request"])
        concurrency = self.workflow["concurrency"]
        self.assertIs(concurrency["cancel-in-progress"], True)
        self.assertIn("github.workflow", concurrency["group"])
        self.assertIn("github.ref", concurrency["group"])

    def test_runtime_and_dependency_cache_contract(self) -> None:
        self.assertEqual(self.job["runs-on"], "ubuntu-24.04")
        self.assertGreater(self.job["timeout-minutes"], 0)
        actions = {step["uses"].split("@")[0]: step for step in self.steps if "uses" in step}
        python = actions["actions/setup-python"]["with"]
        self.assertEqual(python["python-version"], "3.12")
        self.assertEqual(python["cache"], "pip")
        self.assertEqual(python["cache-dependency-path"], "requirements.txt")
        self.assertEqual(actions["actions/setup-java"]["with"]["java-version"], "17")
        checkout = actions["actions/checkout"]["with"]
        self.assertEqual(checkout["fetch-depth"], 0)
        self.assertIs(checkout["persist-credentials"], False)
        self.assertIn("python -m pip install -r requirements.txt", self.commands)

    def test_all_quality_gates_are_required(self) -> None:
        self.assertEqual(self.job["env"]["RUN_SPARK_TESTS"], "1")
        self.assertEqual(self.job["env"]["RUN_WAREHOUSE_INTEGRATION_TESTS"], "0")
        for command in (
            "python -m compileall",
            "python -m unittest discover -s tests -v",
            "dbt parse",
            "dbt compile",
            "--no-introspect",
            "--no-populate-cache",
            "docker compose config --quiet",
            "diff --check",
        ):
            with self.subTest(command=command):
                self.assertIn(command, self.commands)
        self.assertNotIn("continue-on-error", self.job)
        for step in self.steps:
            self.assertTrue(step.get("name"))
            self.assertNotIn("continue-on-error", step)
            self.assertNotIn("if", step)
        self.assertNotIn("|| true", self.commands)

    def test_workflow_has_read_only_scope_and_no_deployment(self) -> None:
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        self.assertNotIn("permissions", self.job)
        self.assertNotIn("services", self.job)
        self.assertNotIn("environment", self.job)
        for forbidden in (
            r"\bsecrets\s*[.\[]",
            r"\bgit\s+(push|reset|clean)\b",
            r"\bdocker\s+(login|push)\b",
            r"\bdocker\s+compose\s+(up|down|run)\b",
            r"\b(kubectl|terraform)\b",
            r"\brm\s+-[a-z]*r",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotRegex(self.text, forbidden)


if __name__ == "__main__":
    unittest.main()
