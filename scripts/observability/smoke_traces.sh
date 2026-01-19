#!/bin/bash
set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Starting End-to-End Trace Smoke Test ===${NC}"

# 1. Generate Traces
echo "Generating traces using verify_otel_setup.py..."
# Export PYTHONPATH
export PYTHONPATH=agent/src:common/src:schema/src

# Capture output to get Trace ID
OUTPUT=$(python3 scripts/verify_otel_setup.py 2>&1)
echo "$OUTPUT"
TRACE_ID=$(echo "$OUTPUT" | grep "Trace ID:" | awk '{print $3}')

if [ -n "$TRACE_ID" ]; then
    echo -e "${GREEN}Captured Trace ID: $TRACE_ID${NC}"
else
    echo -e "${RED}WARNING: Could not capture Trace ID.${NC}"
fi

# 2. Verify Persistence (Postgres)
echo "Verifying Postgres Persistence..."
set +e
python3 scripts/observability/verify_postgres_persistence.py
RC=$?
set -e

if [ $RC -eq 0 ]; then
    echo -e "${GREEN}Postgres Persistence Verified.${NC}"
else
    echo -e "${RED}WARNING: Postgres Persistence Check Failed. See output above.${NC}"
    # Non-blocking failure
fi

# 3. Verify Tempo (Automated)
echo "Verifying Tempo Ingestion..."
if [ -n "$TRACE_ID" ]; then
    # Wait for ingestion
    sleep 3

    # Query Tempo API
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:3200/api/traces/$TRACE_ID")

    if [ "$HTTP_CODE" -eq 200 ]; then
        echo -e "${GREEN}SUCCESS: Trace found in Tempo!${NC}"
    else
        echo -e "${RED}FAILURE: Trace not found in Tempo (HTTP $HTTP_CODE).${NC}"
    fi
else
    echo "Skipping Tempo API check (No Trace ID)."
fi

echo -e "${GREEN}=== Smoke Test Complete ===${NC}"
echo "To verify hierarchical traces in Tempo:"
echo "1. Open Grafana: http://localhost:3000"
echo "2. Go to 'Trace Explorer' dashboard."
if [ -n "$TRACE_ID" ]; then
    echo "3. Search for Trace ID: $TRACE_ID"
fi
echo "4. Click the trace to view the waterfall."
