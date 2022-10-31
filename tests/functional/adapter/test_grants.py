import pytest
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants

from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    write_file,
    relation_from_name,
    get_connection,
)

class TestModelGrantsHive(BaseModelGrants):
    def privilege_grantee_name_overrides(self):
        return {
            "select": "select",
            "insert": "insert",
        }

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        
        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True

user2_incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

class TestIncrementalGrantsHive(BaseIncrementalGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "append",
            }
        }

    def test_incremental_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3

        # Incremental materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_incremental_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, run again without changes
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output  # with space to disambiguate from 'show grants'
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_incremental_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        # assert "revoke " in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, same config, now with --full-refresh
        run_dbt(["--debug", "run", "--full-refresh"])
        assert len(results) == 1
        # whether grants or revokes happened will vary by adapter
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Now drop the schema (with the table in it)
        adapter = project.adapter
        relation = relation_from_name(adapter, "my_incremental_model")
        with get_connection(adapter):
            adapter.drop_schema(relation)

        # Incremental materialization, same config, rebuild now that table is missing
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        # assert "grant " in log_output
        assert "revoke " not in log_output
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        
        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True


class TestSeedGrantsHive(BaseSeedGrants):
    def seeds_support_partial_refresh(self):
        return False

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        
        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True


class TestInvalidGrantsHive(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "RESOURCE_DOES_NOT_EXIST"
        
    def privilege_does_not_exist_error(self):
        return "Action Unknown"

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        
        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True

