#!/bin/bash

# csv file path
INPUT_FILE=~/hive/user_activity_logs_2023-09-01.csv

# Read the csv  file line by line
tail -n +2 $INPUT_FILE | while IFS=, read -r user_id content_id action log_timestamp device region session_id
do
    # Extract year, month, and day from the timestamp 
    YEAR=$(echo $log_timestamp | cut -d'-' -f1)
    MONTH=$(echo $log_timestamp | cut -d'-' -f2)
    DAY=$(echo $log_timestamp | cut -d' ' -f1 | cut -d'-' -f3)

    # Define HDFS path
    HDFS_DIR="/raw/logs/$YEAR/$MONTH/$DAY"

    # Create directories if they don't exist
    hadoop fs -mkdir -p $HDFS_DIR

    # Append this row to the correct partitioned file in HDFS
    echo "$user_id,$content_id,$action,$log_timestamp,$device,$region,$session_id" | hadoop fs -appendToFile - "$HDFS_DIR/user_activity_logs_$YEAR-$MONTH-$DAY.csv"
done

echo "Sucesssssss woohoo!"

