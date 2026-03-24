from pathlib import Path

import pytest

from microdcs.scripts.dataclassgen import _schema_path_to_module_name


@pytest.mark.parametrize(
    ("schema_name", "expected_module_name"),
    [
        ("machinery_jobs.schema.json", "machinery_jobs"),
        ("my-schema.schema.json", "my_schema"),
        ("My Schema.schema.json", "my_schema"),
        ("123-job.schema.json", "_123_job"),
        ("class.schema.json", "class_"),
        ("---.schema.json", "generated"),
    ],
)
def test_schema_path_to_module_name(schema_name: str, expected_module_name: str):
    assert _schema_path_to_module_name(Path(schema_name)).isidentifier()
    assert _schema_path_to_module_name(Path(schema_name)) == expected_module_name
