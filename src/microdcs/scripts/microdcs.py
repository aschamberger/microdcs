import typer

from microdcs.scripts import dataclassgen, init

app = typer.Typer()
app.add_typer(
    dataclassgen.app,
    name="dataclassgen",
    help="Generate python dataclasses from JSON Schema.",
)
app.add_typer(
    init.app,
    name="init",
    help="Initialize a new application.",
)

if __name__ == "__main__":
    app()
