#!/bin/bash

echo "Running Scripts..."
echo "Resetting tables."
python3 create_tables.py
python3 etl.py
echo "Finished."
