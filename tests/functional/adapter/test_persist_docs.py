from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)


class BasePersistDocsHiveCompat(BasePersistDocs):
    """Hive often returns relation/column comments as a single line from metastore."""

    def _assert_common_comments(self, *comments):
        for comment in comments:
            assert comment is not None
            assert "with double quotes" in comment
            assert "abc123" in comment
            assert "Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting" in comment
            assert "/* comment */" in comment
            if "\n" in comment:
                pass
            else:
                assert "statistics are made up" in comment or "reserved -- characters" in comment


class TestPersistDocsHive(BasePersistDocsHiveCompat):
    pass


class TestPersistDocsColumnMissingHive(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumnHive(BasePersistDocsCommentOnQuotedColumn):
    pass
