import json
import re
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


class TestValidationMixinInheritance:
    """Validation mixin must not be added to classes that inherit it from a parent."""

    @pytest.fixture
    def schema_with_inheritance(self, tmp_path: Path) -> Path:
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "InheritanceTest",
            "$defs": {
                "Parent": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "minLength": 1, "maxLength": 50}
                    },
                    "required": ["name"],
                },
                "Child": {
                    "allOf": [
                        {"$ref": "#/$defs/Parent"},
                        {
                            "type": "object",
                            "properties": {"age": {"type": "integer"}},
                        },
                    ]
                },
            },
            "oneOf": [
                {"$ref": "#/$defs/Parent"},
                {"$ref": "#/$defs/Child"},
            ],
        }
        schema_file = tmp_path / "inheritance.schema.json"
        schema_file.write_text(json.dumps(schema))
        return schema_file

    def test_child_class_does_not_get_validation_mixin(
        self, schema_with_inheritance: Path, tmp_path: Path
    ):
        from typer.testing import CliRunner

        from microdcs.scripts.dataclassgen import app

        runner = CliRunner()
        out_dir = tmp_path / "models"
        out_dir.mkdir()
        result = runner.invoke(
            app,
            [
                "dataclasses",
                str(schema_with_inheritance),
                str(out_dir),
                "--validation",
                "--collapse-root-workaround",
            ],
        )
        assert result.exit_code == 0, result.output

        generated = (out_dir / "inheritance.py").read_text()

        # Parent should have DataClassValidationMixin
        parent_match = re.search(r"class Parent\(([^)]+)\)", generated)
        assert parent_match, f"Parent class not found in:\n{generated}"
        assert "DataClassValidationMixin" in parent_match.group(1)

        # Child should NOT have DataClassValidationMixin (inherits from Parent)
        child_match = re.search(r"class Child\(([^)]+)\)", generated)
        assert child_match, f"Child class not found in:\n{generated}"
        assert "DataClassValidationMixin" not in child_match.group(1), (
            f"Child should not have DataClassValidationMixin, got: {child_match.group(1)}"
        )
