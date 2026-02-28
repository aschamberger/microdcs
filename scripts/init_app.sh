curl#!/bin/sh

# This script initializes a new app in the current directory.
#
# It will create the basic structure for a MicroDCS app.
#
# Usage: curl -LsSf https://raw.githubusercontent.com/aschamberger/microdcs/main/scripts/init_app.sh | sh

# Check if uv is installed
if ! command -v uv &> /dev/null
then
    echo "uv could not be found. Please install uv before running this script."
    echo "You can install uv by following the instructions at https://uv.io/getting-started/installation/"
    echo "e.g.: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit
fi

# Create .vscode directory and copy files from MicroDCS
mkdir -p .vscode
curl -LsSf https://raw.githubusercontent.com/aschamberger/microdcs/main/.vscode/settings.json -o .vscode/settings.json
curl -LsSf https://raw.githubusercontent.com/aschamberger/microdcs/main/.vscode/tasks.json -o .vscode/tasks.json

# Install python with uv
#uv python install 3.14.3

# Pin the python version to ensure consistency across environments
uv python pin 3.14

# Create the basic structure for a MicroDCS app
uv init --python >=3.14 --bare
uv sync

# Add MicroDCS as a dependency
uv add git+https://github.com/aschamberger/microdcs

# Add dev dependencies
uv add --dev datamodel-code-generator[ruff] pytest pytest-asyncio pytest-cov

# Create a basic app structure
mkdir -p app
touch app/__init__.py
curl -LsSf https://raw.githubusercontent.com/aschamberger/microdcs/main/app/__main__.py -o app/__main__.py
mkdir -p app/models
mkdir -p app/processors
mkdir -p schemas
mkdir -p deploy
curl -LsSf https://raw.githubusercontent.com/aschamberger/microdcs/main/deploy/k8s.yaml -o deploy/k8s.yaml
mkdir -p tests
curl -LsSf https://raw.githubusercontent.com/aschamberger/microdcs/main/Dockerfile -o Dockerfile

# Sync the dependencies and activate the virtual environment
uv sync
source .venv/bin/activate

echo "App initialized successfully!"