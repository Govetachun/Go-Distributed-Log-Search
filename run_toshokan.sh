#!/bin/bash

# Load environment variables
source tests/env.test

# Debug: Check if environment variables are loaded
echo "DEBUG: S3_ENDPOINT = $S3_ENDPOINT"
echo "DEBUG: AWS_ACCESS_KEY_ID = $AWS_ACCESS_KEY_ID"

# Export environment variables explicitly
export S3_ENDPOINT
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_REGION

# Run toshokan with all arguments passed through
./toshokan "$@"
