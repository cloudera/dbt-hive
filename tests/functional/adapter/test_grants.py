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


@pytest.mark.skip(reason="Not working from the start ie v1.3.1")
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


@pytest.mark.skip(reason="Not working from the start ie v1.3.1")
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
        # assert "revoke " not in log_output
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)

        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True


user2_schema_base_yml = """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

ignore_grants_yml = """
version: 2
seeds:
  - name: my_seed
    config:
      grants: {}
"""

zero_grants_yml = """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select: []
"""


@pytest.mark.skip(reason="Not working from the start ie v1.3.1")
class TestSeedGrantsHive(BaseSeedGrants):
    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)

        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True

    def test_seed_grants(self, project, get_test_users):
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]

        # seed command
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        seed_id = "seed.test.my_seed"
        seed = manifest.nodes[seed_id]
        expected = {select_privilege_name: [test_users[0]]}
        assert seed.config.grants == expected
        # assert "grant " in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again, with no config changes
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        if self.seeds_support_partial_refresh():
            # grants carried over -- nothing should have changed
            assert "revoke " not in log_output
            # assert "grant " not in log_output
        else:
            # seeds are always full-refreshed on this adapter, so we need to re-grant
            assert "grant " in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # change the grantee, assert it updates
        updated_yaml = self.interpolate_name_overrides(user2_schema_base_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again, with --full-refresh, grants should be the same
        run_dbt(["seed", "--full-refresh"])
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # change config to 'grants: {}' -- should be completely ignored
        updated_yaml = self.interpolate_name_overrides(ignore_grants_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        # assert "grant " not in log_output
        manifest = get_manifest(project.project_root)
        seed_id = "seed.test.my_seed"
        seed = manifest.nodes[seed_id]
        expected_config = {}
        expected_actual = {select_privilege_name: [test_users[1]]}
        assert seed.config.grants == expected_config
        if self.seeds_support_partial_refresh():
            # ACTUAL grants will NOT match expected grants
            self.assert_expected_grants_match_actual(project, "my_seed", expected_actual)
        else:
            # there should be ZERO grants on the seed
            self.assert_expected_grants_match_actual(project, "my_seed", expected_config)

        # now run with ZERO grants -- all grants should be removed
        # whether explicitly (revoke) or implicitly (recreated without any grants added on)
        updated_yaml = self.interpolate_name_overrides(zero_grants_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        # if self.seeds_support_partial_refresh():
        #     assert "revoke " in log_output
        expected = {}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again -- dbt shouldn't try to grant or revoke anything
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        # assert "revoke " not in log_output
        # assert "grant " not in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)


my_invalid_model_sql = """
  select 1 as fun
"""

invalid_user_table_model_schema_yml = """
version: 2
models:
  - name: my_invalid_model
    config:
      materialized: table
      grants:
        select: ['invalid_user']
"""

invalid_privilege_table_model_schema_yml = """
version: 2
models:
  - name: my_invalid_model
    config:
      materialized: table
      grants:
        fake_privilege: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


@pytest.mark.skip(reason="Not working from the start ie v1.3.1")
class TestInvalidGrantsHive(BaseInvalidGrants):
    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)

        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True

    def privilege_grantee_name_overrides(self):
        return {
            "select": "select",
            "insert": "insert",
            "fake_privilege": "fake_privilege",
            "invalid_user": "invalid_user",
        }

    def grantee_does_not_exist_error(self):
        return "doesn't exist"

    def privilege_does_not_exist_error(self):
        return "'fake_privilege' in grant role"

    def test_invalid_grants(self, project, get_test_users, logs_dir):
        # failure when grant to a user/role that doesn't exist
        yaml_file = self.interpolate_name_overrides(invalid_user_table_model_schema_yml)
        write_file(yaml_file, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert self.grantee_does_not_exist_error() in log_output

        # failure when grant to a privilege that doesn't exist
        yaml_file = self.interpolate_name_overrides(invalid_privilege_table_model_schema_yml)
        write_file(yaml_file, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert self.privilege_does_not_exist_error() in log_output
