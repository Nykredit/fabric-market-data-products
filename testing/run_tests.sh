#!/bin/bash

# Function to run the teardown script
teardown() {
    echo "Removing symbolic links..."
    .github/scripts/teardown_testing_symlinks.sh
}

# Ensure teardown runs on script exit, even when tests fail
trap teardown EXIT

echo "Creating symbolic links..."
.github/scripts/create_testing_symlinks.sh

# Step 2: Run pytest
echo "Running tests..."
pytest .
