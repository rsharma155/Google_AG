#!/bin/bash
echo "Building SQL Server Metrics Collector..."

# Ensure pyinstaller is installed
pip install pyinstaller

# Build the executable
# --onefile: Create a single executable file
# --name: Name of the output file
# --add-data: Include default config (though we expect external config usually)
# Note: config.yaml is tricky with onefile. 
# Usually we want the user to provide config.yaml next to the binary.
# The app checks for config.yaml in the current working directory, which is fine.

pyinstaller --onefile --name sql_metrics_collector main.py

echo "Build complete. Executable is in dist/sql_metrics_collector"
echo "Don't forget to copy config.yaml to the same directory as the executable!"
