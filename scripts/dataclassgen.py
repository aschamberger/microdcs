import pathlib
import sys
from collections import defaultdict
from typing import Annotated

import typer
from datamodel_code_generator import PythonVersion
from datamodel_code_generator.config import JSONSchemaParserConfig
from datamodel_code_generator.enums import DataModelType
from datamodel_code_generator.format import Formatter
from datamodel_code_generator.model import get_data_model_types
from datamodel_code_generator.model.base import ALL_MODEL
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser
from rich import print
from rich.table import Table

# Add parent directory to path
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

app = typer.Typer()

schemas_path = pathlib.Path(__file__).parent.parent / "schemas"
dataclass_path = pathlib.Path(__file__).parent.parent / "app" / "models"


@app.command()
def index():
    print("[bold purple]Searching schema files ...[/bold purple]")
    parent_path_len = len(str(schemas_path)) + 1

    table = Table()
    table.add_column("File", style="cyan", no_wrap=True)

    for schema_file in sorted(schemas_path.glob("**/*.json")):
        table.add_row(str(schema_file)[parent_path_len:])
    print(table)


@app.command()
def dataclasses(
    schema_file: Annotated[pathlib.Path, typer.Argument()],
    imports: Annotated[list[str], typer.Argument()] = [
        "app.dataclass.DataClassMixin",
        "app.dataclass.DataClassConfig",
        "app.dataclass.DataClassResponseMixin",
        "dataclasses.field",
    ],
    base_class: Annotated[str, typer.Argument()] = "app.dataclass.DataClassMixin",
    config_base_class: Annotated[
        str, typer.Argument()
    ] = "app.dataclass.DataClassConfig",
    validation: Annotated[
        bool, typer.Option(help="Add DataClassValidationMixin to generated classes")
    ] = False,
    request_object: Annotated[
        bool,
        typer.Option(
            help="Add __request_object__ InitVar to generated response classes"
        ),
    ] = False,
    custom_metadata: Annotated[
        bool,
        typer.Option(
            help="Add __custom_metadata__ InitVar[dict[str, Any]] to generated classes"
        ),
    ] = False,
):
    schema_file_path = schemas_path / schema_file
    if not schema_file_path.exists():
        print(f"[bold red]Schema file does not exist: {schema_file_path}[/bold red]")
        raise typer.Exit(code=1)

    print(f"[bold purple]Using schema file: {schema_file}[/bold purple]")

    data_model_types = get_data_model_types(
        DataModelType.DataclassesDataclass, target_python_version=PythonVersion.PY_314
    )
    validation_mixin_import = "app.dataclass.DataClassValidationMixin"
    if validation and validation_mixin_import not in imports:
        imports.append(validation_mixin_import)
    initvar_import = "dataclasses.InitVar"
    if (request_object or custom_metadata) and initvar_import not in imports:
        imports.append(initvar_import)
    typing_any_import = "typing.Any"
    if custom_metadata and typing_any_import not in imports:
        imports.append(typing_any_import)

    extra_template_data = {
        ALL_MODEL: {
            "config_base_class": config_base_class.split(".")[-1]
            if config_base_class
            else None,
            "validation_mixin_class": "DataClassValidationMixin"
            if validation
            else None,
            "request_object": request_object,
            "custom_metadata": custom_metadata,
        }
    }
    config = JSONSchemaParserConfig(
        target_python_version=PythonVersion.PY_314,
        use_union_operator=True,
        use_standard_collections=True,
        formatters=[Formatter.RUFF_FORMAT, Formatter.RUFF_CHECK],
        custom_template_dir=pathlib.Path(__file__).parent / "template",
        data_model_type=data_model_types.data_model,
        data_model_root_type=data_model_types.root_model,
        data_model_field_type=data_model_types.field_model,
        data_type_manager_type=data_model_types.data_type_manager,
        dump_resolve_reference_action=data_model_types.dump_resolve_reference_action,
        field_constraints=True,
        keyword_only=True,
        use_subclass_enum=True,
        capitalise_enum_members=True,
        snake_case_field=True,
        use_field_description=True,
        use_schema_description=True,
        use_title_as_name=True,
        additional_imports=imports,
        base_class=base_class,
        extra_template_data=defaultdict(dict, extra_template_data),
    )
    parser = JsonSchemaParser(schema_file_path.read_text(), config=config)
    result = parser.parse()
    out = dataclass_path / (
        schema_file_path.name.replace("".join(schema_file_path.suffixes), ".py")
    )
    f = out.open("w")
    f.write(f'# Auto-generated from "{schema_file}". Do not modify!\n')
    f.write(result)
    f.close()
    print(f"[bold green]Wrote dataclasses to: {out}[/bold green]")


if __name__ == "__main__":
    app()
