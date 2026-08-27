"""Unit tests for CronJob deployment (F1-CJ-4: Test CronJob deployment).

BUD-127 (F1-05): the standalone hourly `f1-fetch-results` and per-minute
`f1-lock-races` CronJobs were superseded by the `f1-race-manager` state
machine (see base/race-manager-cronjob.yaml) and their manifests were
removed — they were never wired into base/kustomization.yaml, so they were
dead config that could mislead someone into thinking two extra jobs were
deployed. `TestKustomizationWiring` below guards against the pipeline
silently dropping out of the deployed manifest set again.
"""

import pytest
import os
import yaml


class TestRaceManagerCronJob:
    """Test cases for race-manager CronJob."""

    @pytest.fixture
    def race_manager_spec(self):
        """Load the race-manager CronJob YAML."""
        yaml_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'base', 'race-manager-cronjob.yaml'
        )
        with open(yaml_path) as f:
            return yaml.safe_load(f)

    def test_race_manager_cronjob_exists(self, race_manager_spec):
        """Race-manager CronJob is defined.
        
        Given the race-manager CronJob YAML exists
        When parsed
        Then it should be a valid CronJob resource
        """
        assert race_manager_spec['kind'] == 'CronJob', "Should be a CronJob resource"

    def test_race_manager_uses_race_manager_script(self, race_manager_spec):
        """Race-manager runs race_manager.py.
        
        Given the CronJob container
        When checked
        Then command should run race_manager.py
        """
        containers = race_manager_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        container = containers[0]
        
        command_str = ' '.join(container['command'])
        assert 'python3' in command_str, "Should run python3"
        assert 'race_manager.py' in command_str, "Should run race_manager.py"

    def test_race_manager_has_pvc_mount(self, race_manager_spec):
        """Race-manager has PVC mounted at /data.

        Given the race-manager CronJob
        When checked
        Then /data should be mounted
        """
        containers = race_manager_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        container = containers[0]

        volume_mounts = container.get('volumeMounts', [])
        data_mount = next((m for m in volume_mounts if m['mountPath'] == '/data'), None)
        assert data_mount is not None, "Should have /data mount"

    def test_race_manager_schedule_frequent_enough_for_30min_ac(self, race_manager_spec):
        """BUD-127: outer cron cadence must be <= 30 min so results/scores can
        land within the 'correct within 30 minutes of official results' AC.

        Given the race-manager CronJob schedule
        When parsed as a 5-field cron expression
        Then the minute field must fire at least every 30 minutes
        """
        schedule = race_manager_spec['spec']['schedule']
        minute_field = schedule.split()[0]
        assert minute_field.startswith('*/'), (
            f"Expected a '*/N' minute cadence, got {minute_field!r} in schedule {schedule!r}"
        )
        step = int(minute_field[2:])
        assert step <= 30, f"CronJob only fires every {step} min — too slow for the 30-min AC"


class TestKustomizationWiring:
    """BUD-127 (F1-05): guard that the score-update pipeline is actually
    deployed. The fetch-results and lock-races CronJobs were superseded by
    f1-race-manager but were never removed from base/kustomization.yaml
    resources in the first place — this pins down what IS deployed so a
    future edit can't silently drop the pipeline out of kustomization.
    """

    @pytest.fixture
    def kustomization(self):
        yaml_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'base', 'kustomization.yaml'
        )
        with open(yaml_path) as f:
            return yaml.safe_load(f)

    def test_race_manager_cronjob_is_deployed(self, kustomization):
        """The score-update pipeline's CronJob must be a base resource."""
        assert 'race-manager-cronjob.yaml' in kustomization['resources'], (
            "f1-race-manager is the only CronJob that fetches results and "
            "updates scores/leaderboard — it must stay in base/kustomization.yaml"
        )

    def test_orphaned_cronjob_manifests_removed(self):
        """The superseded fetch-results/lock-races manifests should not
        reappear — they duplicated race-manager's job without its state
        machine and were never deployed via kustomization anyway."""
        base_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'base')
        for stale_name in ('fetch-results-cronjob.yaml', 'lock-races-cronjob.yaml'):
            assert not os.path.exists(os.path.join(base_dir, stale_name)), (
                f"{stale_name} was removed as dead config in BUD-127 — "
                "re-add only alongside a kustomization.yaml resources entry"
            )


class TestPVCManifest:
    """Test cases for PVC manifest."""

    @pytest.fixture
    def pvc_spec(self):
        """Load the PVC YAML."""
        yaml_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'base', 'pvc.yaml'
        )
        with open(yaml_path) as f:
            return yaml.safe_load(f)

    def test_pvc_exists(self, pvc_spec):
        """PVC resource is defined.
        
        Given the PVC YAML
        When parsed
        Then it should be a valid PVC resource
        """
        assert pvc_spec['kind'] == 'PersistentVolumeClaim'

    def test_pvc_storage_request(self, pvc_spec):
        """PVC requests 1Gi storage.
        
        Given the PVC spec
        When checked
        Then storage request should be 1Gi
        """
        storage = pvc_spec['spec']['resources']['requests']['storage']
        assert storage == '1Gi', "Should request 1Gi storage"

    def test_pvc_access_mode(self, pvc_spec):
        """PVC uses ReadWriteOnce access mode.
        
        Given the PVC spec
        When checked
        Then accessModes should be ReadWriteOnce
        """
        assert 'ReadWriteOnce' in pvc_spec['spec']['accessModes']


class TestCronJobScriptExecution:
    """Test that cron scripts can execute successfully."""

    def test_fetch_results_script_has_test_api_option(self):
        """Fetch results script supports --test-api flag.
        
        Given the fetch_race_results.py script
        When checked
        Then it should have a --test-api option for validation
        """
        script_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'cron', 'fetch_race_results.py'
        )
        with open(script_path) as f:
            content = f.read()
        
        assert '--test-api' in content, "Script should support --test-api flag"
        assert 'run_test_api_fetch' in content, "Script should have test API function"

    def test_race_manager_script_importable(self):
        """Race manager script can be imported without errors.
        
        Given the race_manager.py script
        When imported in test environment
        Then it should not raise ImportError
        """
        import sys
        import os
        cron_path = os.path.join(os.path.dirname(__file__), '..', '..', 'cron')
        src_path = os.path.join(os.path.dirname(__file__), '..', '..', 'src')
        sys.path.insert(0, cron_path)
        sys.path.insert(0, src_path)

        # Should not raise any import errors (race_manager imports openf1
        # from src/, hence src_path above)
        import race_manager
        assert hasattr(race_manager, 'main'), "race_manager should have main()"


class TestKubernetesManifestsValid:
    """Validate all Kubernetes manifests are valid YAML."""

    def test_all_base_manifests_valid_yaml(self):
        """All Kubernetes manifests are valid YAML.
        
        Given the base/ directory
        When each YAML file is parsed
        Then all should be valid YAML
        """
        import glob
        
        base_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'base')
        yaml_files = glob.glob(os.path.join(base_dir, '*.yaml'))
        
        assert len(yaml_files) > 0, "Should have YAML files in base/"
        
        for yaml_file in yaml_files:
            with open(yaml_file) as f:
                # Use safe_load_all for multi-document YAML files
                docs = list(yaml.safe_load_all(f))
                assert len(docs) > 0, f"{yaml_file} should have at least one document"
                for doc in docs:
                    assert doc is not None, f"{yaml_file} should parse as YAML"
                    assert 'kind' in doc, f"{yaml_file} should have a 'kind' field"
