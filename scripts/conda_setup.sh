#!/bin/bash

set -eufo pipefail

echo "Setting up nba-basketball-dbt conda env"

# Config shell session to work with conda
. ~/miniconda3/etc/profile.d/conda.sh

conda deactivate
conda env remove -n nba-basketball-dbt
conda create -yqn nba-basketball-dbt python=3.10
conda activate nba-basketball-dbt
python -m pip install dbt-duckdb=='1.7.1'

echo "Finished, now spin up your new conda environment with 'conda activate nba-basketball-dbt'"
