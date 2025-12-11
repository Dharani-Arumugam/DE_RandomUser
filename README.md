# Realtime Data Streaming | End-to-End Data Engineering Project

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)

## Introduction

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline. It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease of deployment and scalability.

## System Architecture
![System Architecture](https://github.com/Dharani-Arumugam/RandomUserAPI_EndToEnd/blob/main/Data%20engineering%20architecture.png)


The project is designed with the following components:

- **Data Source**: `randomuser.me` API is used to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline of sending the data to Kafka
- **Apache Kafka and Zookeeper**: Used for streaming data from Kafka broker to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.
## Project Structure 

~~~
DE_RandomUser/
│
├── airflow/
│   ├── dags/
│   │   └── kafka_stream.py
│   │
│   ├── logs/                     # Airflow logs generated automatically
│   │
│   ├── script/
│   │   └── entrypoint.sh         # Custom Airflow startup script
│   │
│   ├── requirements.txt          # Python deps for Airflow
│   └── airflow.cfg               # Airflow config override
│
├── spark/
│   ├── app/
│   │   └── spark_stream.py       # Spark Structured Streaming app
│   │
│   ├── conf/
│   │   └── spark_env.sh          # Spark environment variables
│   │
│   ├── extra-jars/               # Kafka + Cassandra connectors
│   │   └── jars/                 # (Place your .jar files here)
│   │
│   └── Dockerfile                # Spark custom image
│
├── docker-compose.yml            # Orchestrates Airflow + Kafka + Spark
│
└── README.md                     # Project description & setup

~~~
## Technologies

 - airflow 3.1.3
 - kafka 3.4
 - spark 3.5.1
 - cassandra 4.1
 - docker 
   
## Learnings

- Setting up a data pipeline with Apache Airflow
- Real-time data streaming with Apache Kafka
- Distributed synchronization with Apache Zookeeper
- Data processing techniques with Apache Spark
- Data storage solutions with Cassandra 
- Containerizing your entire data engineering setup with Docker

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Spark
- Cassandra
- PostgreSQL
- Docker

 
