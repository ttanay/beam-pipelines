# A streaming pipeline using PubSub and BigQuery

**Problem Statement:**
A source produces messages, each being assigned a number in range [MIN, MAX]. Write a Pipeline using Apache Beam to read these messages and calculate the sum of the numbers assigned to messages for every 10 minute window. Write this sum to bigquery using streaming inserts.
Bonus: Plot the sum for each 10-minute.
