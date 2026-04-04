"""Unit tests for CronJob deployment (F1-CJ-4: Test CronJob deployment)."""

import pytest
import os
import yaml


class TestFetchResultsCronJob:
    """Test cases for fetch-results CronJob (CJ-012, CJ-013)."""

    @pytest.fixture
    def cronjob_spec(self):
        """Load the fetch-results CronJob YAML."""
        yaml_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'base', 'fetch-results-cronjob.yaml'
        )
        with open(yaml_path) as f:
            return yaml.safe_load(f)

    def test_cj_012_fetch_results_cronjob_exists(self, cronjob_spec):
        """CJ-012: Hourly cron runs fetch_race_results.py.
        
        Given the fetch-results CronJob YAML exists
        When parsed
        Then it should be a valid CronJob resource
        """
        assert cronjob_spec['kind'] == 'CronJob', "Should be a CronJob resource"
        assert cronjob_spec['apiVersion'] == 'batch/v1', "Should use batch/v1 API"

    def test_cj_012_hourly_schedule_configured(self, cronjob_spec):
        """CJ-012: Hourly cron schedule configured.
        
        Given the CronJob is defined
        When checked
        Then schedule should run hourly (at minute 5)
        """
        schedule = cronjob_spec['spec']['schedule']
        assert schedule == "5 * * * *", f"Expected hourly schedule '5 * * * *', got {schedule}"

    def test_cj_012_fetch_script_command(self, cronjob_spec):
        """CJ-012: CronJob runs fetch_race_results.py.
        
        Given the CronJob container is defined
        When checked
        Then command should run fetch_race_results.py
        """
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        assert len(containers) == 1
        container = containers[0]
        
        command_str = ' '.join(container['command'])
        assert 'python3' in command_str, "Should run python3"
        assert 'fetch_race_results.py' in command_str, "Should run fetch_race_results.py"

    def test_cj_013_pvc_mounted_at_data(self, cronjob_spec):
        """CJ-013: PVC mounted at /data.
        
        Given the CronJob is defined
        When checked
        Then volumeMounts should have /data mounted from PVC
        """
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        container = containers[0]
        
        volume_mounts = container.get('volumeMounts', [])
        assert len(volume_mounts) > 0, "Should have volume mounts"
        
        data_mount = next((m for m in volume_mounts if m['mountPath'] == '/data'), None)
        assert data_mount is not None, "Should have /data mount"
        assert data_mount['name'] == 'data', "Should mount 'data' volume"

    def test_cj_013_pvc_claim_configured(self, cronjob_spec):
        """CJ-013: PVC claim configured.
        
        Given the CronJob volumes are defined
        When checked
        Then PVC should reference f1-predictor-data
        """
        volumes = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec'].get('volumes', [])
        assert len(volumes) > 0, "Should have volumes"
        
        data_volume = next((v for v in volumes if v['name'] == 'data'), None)
        assert data_volume is not None, "Should have 'data' volume"
        assert 'persistentVolumeClaim' in data_volume, "Should be a PVC"
        assert data_volume['persistentVolumeClaim']['claimName'] == 'f1-predictor-data', \
            "PVC claim should be f1-predictor-data"

    def test_cj_013_database_path_env_set(self, cronjob_spec):
        """CJ-013: DATABASE_PATH environment variable set to /data.
        
        Given the CronJob container env vars
        When checked
        Then DATABASE_PATH should be /data/f1_predictions.db
        """
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        container = containers[0]
        
        env = {e['name']: e['value'] for e in container.get('env', [])}
        assert 'DATABASE_PATH' in env, "Should have DATABASE_PATH env var"
        assert env['DATABASE_PATH'] == '/data/f1_predictions.db', \
            "DATABASE_PATH should be /data/f1_predictions.db"

    def test_cj_013_job_completes_with_concurrency_policy(self, cronjob_spec):
        """CJ-013: Job concurrency policy prevents overlaps.
        
        Given the CronJob spec
        When checked
        Then concurrencyPolicy should be Forbid (prevents overlapping runs)
        """
        assert cronjob_spec['spec'].get('concurrencyPolicy') == 'Forbid', \
            "ConcurrencyPolicy should be Forbid"

    def test_cj_013_successful_jobs_history_limit(self, cronjob_spec):
        """CJ-013: Successful jobs history is limited.
        
        Given the CronJob spec
        When checked
        Then successfulJobsHistoryLimit should be set
        """
        assert 'successfulJobsHistoryLimit' in cronjob_spec['spec'], \
            "Should have successfulJobsHistoryLimit"
        assert cronjob_spec['spec']['successfulJobsHistoryLimit'] == 2


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
        sys.path.insert(0, cron_path)
        
        # Should not raise any import errors
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
