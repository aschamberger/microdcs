import shutil
import subprocess
from pathlib import Path

import typer
from rich import print

GITHUB_REPO = "https://github.com/aschamberger/microdcs"

# All template files are bundled inside the init/ directory so they are
# available when MicroDCS is installed as a package (not just from a local clone).
_INIT_DIR = Path(__file__).parent / "init"

PYPROJECT_EXTRA_SECTIONS = """
[build-system]
requires = ["uv_build>=0.10.7,<0.11.0"]
build-backend = "uv_build"

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "integration: integration tests requiring external services (MQTT, Redis)",
]
"""

DIRS = [
    "app/models",
    "app/processors",
    "schemas",
    "deploy",
    "tests",
    ".vscode",
    ".github",
]

TOUCH_FILES = [
    "app/__init__.py",
    "app/py.typed",
    "app/models/__init__.py",
    "app/processors/__init__.py",
]

# Template files bundled in init/ mapped to their destination in the new project.
TEMPLATE_FILES = {
    "copilot-instructions.md": ".github/copilot-instructions.md",
    "settings.json": ".vscode/settings.json",
    "tasks.json": ".vscode/tasks.json",
    "__main__.py": "app/__main__.py",
    "k8s.yaml": "deploy/k8s.yaml",
    "Dockerfile": "Dockerfile",
}

app = typer.Typer()


def _check_uv() -> None:
    """Check if uv is installed and available on PATH."""
    if not shutil.which("uv"):
        print(
            "[bold red]uv could not be found. "
            "Please install uv before running this command.[/bold red]"
        )
        print(
            "You can install uv by following the instructions at "
            "https://docs.astral.sh/uv/getting-started/installation/"
        )
        print("e.g.: [cyan]curl -LsSf https://astral.sh/uv/install.sh | sh[/cyan]")
        raise typer.Exit(1)


def _run(cmd: list[str], cwd: Path) -> None:
    """Run a subprocess command, printing it first."""
    print(f"[dim]$ {' '.join(cmd)}[/dim]")
    subprocess.run(cmd, cwd=cwd, check=True)


def _copy_file(src: Path, dest: Path) -> None:
    """Copy a file to *dest*, creating parent directories as needed."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)


def _append_pyproject_sections(pyproject: Path) -> None:
    """Append build-system and pytest sections to pyproject.toml."""
    content = pyproject.read_text()
    if "[build-system]" not in content:
        pyproject.write_text(content.rstrip("\n") + "\n" + PYPROJECT_EXTRA_SECTIONS)


@app.command()
def init(path: Path | None = None) -> None:
    """Initialize a new MicroDCS application project."""
    print("[bold purple]Initializing new MicroDCS project ...[/bold purple]")

    if path is None:
        path = Path.cwd()
        print(f"Using current directory: {path}")
    else:
        path = path.resolve()
        path.mkdir(parents=True, exist_ok=True)
        print(f"Using provided path: {path}")

    _check_uv()

    # Pin Python version and scaffold project
    _run(["uv", "python", "pin", "3.14"], cwd=path)
    _run(["uv", "init", "--python=>=3.14", "--bare"], cwd=path)
    _run(["uv", "sync"], cwd=path)

    # Add MicroDCS and dev dependencies
    _run(["uv", "add", f"git+{GITHUB_REPO}"], cwd=path)
    _run(
        [
            "uv",
            "add",
            "--dev",
            "datamodel-code-generator[ruff]",
            "pytest",
            "pytest-asyncio",
            "pytest-cov",
        ],
        cwd=path,
    )

    # Create directory structure
    for d in DIRS:
        (path / d).mkdir(parents=True, exist_ok=True)

    # Create empty marker / __init__ files
    for f in TOUCH_FILES:
        (path / f).touch()

    # Copy bundled template files
    for template_name, dest_rel in TEMPLATE_FILES.items():
        _copy_file(_INIT_DIR / template_name, path / dest_rel)

    # Seed schemas directory
    readme = path / "schemas" / "README.md"
    if not readme.exists():
        readme.write_text("JSON Schema files ...\n")

    # Append build-system and pytest config to pyproject.toml
    _append_pyproject_sections(path / "pyproject.toml")

    # Final dependency sync
    _run(["uv", "sync"], cwd=path)

    print("[bold green]MicroDCS project initialized successfully![/bold green]")


if __name__ == "__main__":
    app()
