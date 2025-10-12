#!/bin/bash
# Backfill historical weather data in chunks
# This script loads data in monthly chunks to avoid timeouts and allow progress tracking

set -e

WORKERS=${1:-8}
BATCH_SIZE=${2:-200}
YEAR=${3:-2024}

echo "=== Historical Weather Data Backfill ==="
echo "Year: $YEAR"
echo "Workers: $WORKERS"
echo "Batch size: $BATCH_SIZE"
echo ""

# Load data month by month (more manageable chunks)
months=(
    "01-31:31"  # January
    "02-28:28"  # February (adjust for leap year if needed)
    "03-31:31"  # March
    "04-30:30"  # April
    "05-31:31"  # May
    "06-30:30"  # June
    "07-31:31"  # July
    "08-31:31"  # August
    "09-30:30"  # September
    "10-31:31"  # October
    "11-30:30"  # November
    "12-31:31"  # December
)

for month_data in "${months[@]}"; do
    IFS=':' read -r month days <<< "$month_data"

    echo "-----------------------------------"
    echo "Loading $YEAR-$month ($days days)..."
    echo "-----------------------------------"

    docker compose exec -T app python -m src.ingest_weather_raw \
        --date "$YEAR-$month" \
        --days "$days" \
        --workers "$WORKERS" \
        --batch-size "$BATCH_SIZE"

    echo "✓ Completed $YEAR-$month"
    echo ""

    # Run transformation after each month
    echo "Transforming data for $YEAR-$month..."
    docker compose exec -T app python -c "
import sys
sys.path.insert(0, '/app/src')
from database import transform_all_raw_data
stats = transform_all_raw_data()
print(f'✓ Transformed: {stats}')
"
    echo ""
done

echo "========================================="
echo "✓ Historical backfill complete for $YEAR"
echo "========================================="
