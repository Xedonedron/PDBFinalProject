# PDBFinalProject
This is a Final Project repository for Pengolahan Data Besar subject, doing sentiment analysis on twitter data streams to find iut public reaction to the latest films, we are using Twitter API for pull the tweet into Kafka, and send it to Spark for sentiment analysis.

## Prerequisite:
1. Twitter API
2. Apache Spark 3.3.0
3. Confluent Kafka 7.3.0
4. Python 3.10.6

## How to run?
1. Installing Docker Desktop
install docker on your windows, i won't explain it here 
2. Launching Apache Kafka via a Docker container
You should start the docker container for launching the Kafka by entering these command ```docker-compose -f zk-single-kafka-single.yml up -d``` and make sure the Docker is running by see the status ```docker-compose -f zk-single-kafka-single.yml ps```
3. Start your Kafka service
You can start using Kafka by entering this command ```docker exec -it kafka1 /bin/bash```. After that, you can start your Kafka command such like create, describe the Kafka topics, and use producer to inegst the tweet into the topic, and also send it to the consumer.
4. Create an Kafka Topic
Before you run your real-time sentiment analysis tweet, you should create a topic first. You can create a topic by run this command ```kafka-topics --create --topic twitter --bootstrap-server localhost:9092```
5. Run a producer to get streaming tweet
Make sure you have changed the ```search_term``` for your own analysis, you can start streaming the Twitter data by run the producer.py (```python producer.py```)
6. Time for explode your console
This is the last step, you can execute the consumer.ipynb for start streaming the data from the topic created before to the Spark for sentiment analysis.

## Untuk pengembangan berikutnya
Sebelum finalisasi, kami berpikir bahwa data yang sudah dilakukan sentimen analisis secara real-time dari data streaming Twitter ini bisa divisualisasikan secara otomatis juga dengan cara menggunakan ```Apache Airflow``` untuk diintegrasikan dengan ```Tableau/PowerBI```, serta data yang disimpan di dalam topic Kafka lebih baik disimpan ke dalam HDFS secara otomatis dengan menjadikan HDFS sebagai consumer atau menggunakan library ```pydoop, snakebite, hdfs```.
