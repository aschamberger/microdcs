import json
import pathlib
import sys
from collections import defaultdict
from typing import Annotated, Any

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

schemas_path = pathlib.Path(__file__).parent.parent.parent.parent / "schemas"
models_path = pathlib.Path(__file__).parent.parent / "models"


app = typer.Typer()


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
    imports: Annotated[list[str], typer.Option()] = [
        "microdcs.dataclass.DataClassMixin",
        "microdcs.dataclass.DataClassConfig",
        "microdcs.dataclass.DataClassResponseMixin",
        "dataclasses.field",
    ],
    base_class: Annotated[str, typer.Option()] = "microdcs.dataclass.DataClassMixin",
    config_base_class: Annotated[
        str, typer.Option()
    ] = "microdcs.dataclass.DataClassConfig",
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
    hidden_fields: Annotated[
        list[str],
        typer.Option(
            help="Add hidden fields to generated classes (format: name->type, e.g. _header_type->str)"
        ),
    ] = [],
    init_fields: Annotated[
        list[str],
        typer.Option(
            help="Add InitVar fields to generated classes (format: name->type, e.g. mystatus->MyStatus)"
        ),
    ] = [],
    add_mixin: Annotated[
        list[str],
        typer.Option(
            help="Add a mixin to a specific class (format: ClassName->MixinName, e.g. ISA95JobOrderDataType->JobStateMixin)"
        ),
    ] = [],
):
    schema_file_path = schemas_path / schema_file
    if not schema_file_path.exists():
        print(f"[bold red]Schema file does not exist: {schema_file_path}[/bold red]")
        raise typer.Exit(code=1)

    print(f"[bold purple]Using schema file: {schema_file}[/bold purple]")

    data_model_types = get_data_model_types(
        DataModelType.DataclassesDataclass, target_python_version=PythonVersion.PY_314
    )
    validation_mixin_import = "microdcs.dataclass.DataClassValidationMixin"
    if validation and validation_mixin_import not in imports:
        imports.append(validation_mixin_import)
    initvar_import = "dataclasses.InitVar"
    if (
        request_object or custom_metadata or init_fields
    ) and initvar_import not in imports:
        imports.append(initvar_import)
    typing_any_import = "typing.Any"
    if custom_metadata and typing_any_import not in imports:
        imports.append(typing_any_import)

    parsed_hidden_fields = []
    for hf in hidden_fields:
        if "->" not in hf:
            print(
                f"[bold red]Invalid hidden field format: {hf} (expected name->type)[/bold red]"
            )
            raise typer.Exit(code=1)
        name, type_hint = hf.split("->", 1)
        parsed_hidden_fields.append({"name": name.strip(), "type": type_hint.strip()})

    parsed_init_fields = []
    for ivf in init_fields:
        if "->" not in ivf:
            print(
                f"[bold red]Invalid init field format: {ivf} (expected name->type)[/bold red]"
            )
            raise typer.Exit(code=1)
        name, type_hint = ivf.split("->", 1)
        parsed_init_fields.append({"name": name.strip(), "type": type_hint.strip()})

    parsed_class_mixins: dict[str, list[str]] = defaultdict(list)
    for am in add_mixin:
        if "->" not in am:
            print(
                f"[bold red]Invalid mixin format: {am} (expected ClassName->MixinName)[/bold red]"
            )
            raise typer.Exit(code=1)
        class_name, mixin_name = am.split("->", 1)
        parsed_class_mixins[class_name.strip()].append(mixin_name.strip())
    if parsed_class_mixins:
        for class_name, mixins in parsed_class_mixins.items():
            print(
                f"[bold cyan]Adding mixin(s) {', '.join(mixins)} to class: {class_name}[/bold cyan]"
            )

    # Parse schema to find $defs with x-cloudevent-type
    schema_data = json.loads(schema_file_path.read_text())
    cloudevent_defs = {
        name
        for name, defn in schema_data.get("$defs", {}).items()
        if "x-cloudevent-type" in defn
    }
    if cloudevent_defs:
        print(
            f"[bold cyan]Cloud event types found: {', '.join(sorted(cloudevent_defs))}[/bold cyan]"
        )

    # Apply config_base_class and validation_mixin_class to all models.
    # Apply hidden_fields, init_fields, request_object, custom_metadata
    # only to models that have x-cloudevent-type.
    extra_template_data: dict[str, dict[str, Any]] = {
        ALL_MODEL: {
            "config_base_class": config_base_class.split(".")[-1]
            if config_base_class
            else None,
            "validation_mixin_class": "DataClassValidationMixin"
            if validation
            else None,
        }
    }
    cloudevent_model_data = {
        "request_object": request_object,
        "custom_metadata": custom_metadata,
        "hidden_fields": parsed_hidden_fields,
        "init_fields": parsed_init_fields,
    }
    for def_name in cloudevent_defs:
        extra_template_data[def_name] = cloudevent_model_data.copy()
    for class_name, mixins in parsed_class_mixins.items():
        if class_name not in extra_template_data:
            extra_template_data[class_name] = {}
        extra_template_data[class_name]["mixins"] = mixins
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
    out = models_path / (
        schema_file_path.name.replace("".join(schema_file_path.suffixes), ".py")
    )
    f = out.open("w")
    f.write(f'# Auto-generated from "{schema_file}". Do not modify!\n')
    f.write(result)
    f.close()
    print(f"[bold green]Wrote dataclasses to: {out}[/bold green]")


if __name__ == "__main__":
    app()
