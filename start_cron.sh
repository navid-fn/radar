#!/bin/sh
# This script starts the logging service and then the cron service.

# Start the rsyslog service in the background
rsyslogd

# Start the cron service in the foreground
# This will keep the container running
echo "Starting cron daemon..."
cron -f
