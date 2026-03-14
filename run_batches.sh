#!/bin/bash
source venv/bin/activate

echo "Starting continuous batch processing..."

while true; do
    echo "=================================================="
    echo "Launching next batch of 100 tickers at $(date)"
    echo "=================================================="
    
    # Run the scanner and capture output to check for completion
    python3 batch_scanner.py 100 --mode small_mid | tee /tmp/batch_output.txt
    
    if grep -q "All candidates have been processed" /tmp/batch_output.txt; then
        echo "=================================================="
        echo "🎉 ALL BATCHES COMPLETED! Finished at $(date)"
        echo "=================================================="
        break
    fi
    
    if grep -q "No candidates found" /tmp/batch_output.txt; then
        echo "=================================================="
        echo "No candidates found from Finviz. Exiting."
        echo "=================================================="
        break
    fi
    
    echo "Batch finished. Waiting 5 seconds before starting the next..."
    sleep 5
done
