import pytest

from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences

class TestBaseDocsHive(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=profile_user,
            id_type="integer",
            text_type="text",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
            seed_stats=no_stats()
        )

class TestBaseDocsGenRefsHive(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=profile_user,
            id_type="integer",
            text_type="text",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
            seed_stats=no_stats()
        )