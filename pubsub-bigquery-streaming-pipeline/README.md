# A streaming pipeline using PubSub and BigQuery

**Problem Statement:**
A source produces messages, each being assigned a number in range [MIN, MAX]. Write a Pipeline using Apache Beam to read these messages and calculate the sum of the numbers assigned to messages for every X minute window. Write this sum to bigquery using streaming inserts.

Bonus:
1. Multiple producers
2. Visualize data with DataStudio

## Experiment 1
A simple pipeline with minimum transformations and extras to write to BigQuery from PubSub.  
**Schema:**  
Message - | id | sum | window_end_time |  
**Notes:**  
The pipeline prints everything correctly, but, data is not written to BigQuery.

## Experiment 2
Multiple producers publish messages to PubSub.  
**Schema:**  
Message - | id | data | attributes | publisher_id | window_id |  
Window - | id | start_time | end_time |   
Publisher - | id | start_time | end_time |

## Experiment 3
Visualize data with Data Studio

## Future Scope / Improvements
1. Dockerize publishers and pipelines
2. Use Docker Compose to start multiple publisher and a pipeline
3. Can be used as a playground to test ideas quickly
