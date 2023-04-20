# 🚂 RailScope

🚦 Real-Time Performance Analysis for UK National Rail 🛤️

## 📝 Project description:

The RTPPM API provides real-time data on the Public Performance Measure (PPM), including metrics such as on-time trains, late trains, canceled/very late trains, and an indicator of overall performance. 

The data from this API will be transformed and processed for non-technical users.

The RTPPM API data is expected to arrive every 60 seconds

The dashboard will display key performance indicators such as on-time percentage, cancellations, and delays.

The dashboard will be designed for use by both operations managers and other stakeholders who need to monitor the performance of the National Rail system, as well as the general public.

 By making the dashboard publicly available, anyone can monitor the performance of the National Rail system without having to study the technical aspects of the underlying APIs.

## 🚆 Project Analysis

## [🔍 Explore the Analysis](./analysis/README.md)
![National Dashboard](./images/national_dashboard.png)

Welcome to the project analysis section! Here, you can find all the in-depth analyses I have conducted on the UK Network Rail Feed dataset to evaluate the performance of the railway system. These analyses cover various aspects such as Public Performance Measure (PPM), trends, and performance distributions, providing valuable insights for stakeholders to make informed decisions on service improvements.


## 📚 Project Glossary
Discover the definitions and explanations of terminology used throughout the project. This glossary provides clarity on key terms, making it easier to understand the context and purpose of the analyses.

[🔎 Explore the Glossary](./glossary/README.md)

## 🛠️ Pre requisites:

1. Network Rail Account

    First, register for an account by visiting https://publicdatafeeds.networkrail.co.uk/. 
    You will receive a confirmation email 📧. Follow the instructions to log in and change your password. When your account is active, you can connect to the service. Your account may be in one of three states - the system will send you an email when your account is activated and able to access feeds.
2. Terraform [installation guide](https://developer.hashicorp.com/terraform/downloads?product_intent=terraform)
3. Docker [installation guide](https://www.docker.com)


## ☁️ Cloud 
In this section we will leaverage all the processing to GCP servers 

!- **Still work in progress** -!

## 💻 Local
If you want to run this project "locally" you can go to this section, please take in mind that you will need to use some cloud services from gcp like GCS for data lake storage, BigQuery for Data warehousing and Google Data Studio to create the dashboards

## Steps to run this project

### 1. Credentials and Cloud Infrastructure
In order to execute this project, you'll need to provide some credentials which will allow you to store the data in GCP.

1. Create a service account in IAM with the following permissions:
    1. BigQuery Admin
    2. Storage Admin
2. Download the json key
3. Export the json key directory into a enviroment variable
```bash
- MacOS
export GOOGLE_APPLICATION_CREDENTIALS = YOUR_JSON_KEY_DIRECTORY

- Windows
set GOOGLE_APPLICATION_CREDENTIALS = YOUR_JSON_KEY_DIRECTORY

- Linux
export GOOGLE_APPLICATION_CREDENTIALS=YOUR_JSON_KEY_DIRECTORY
```
4. Open a new terminal and move into the following dir = **/RailScope/national_rail_project/cloud/Terraform**
5. Initialize Terraform
```bash
terraform init
```
6. If prompted to authorize Cloud Shell, click Authorize
7. View the Terraform execution plan
```bash
terraform plan
```
8.  Apply the changes
```bash
terraform apply
```
9. Type **yes** and press **Enter**

### 2. Kafka server
1. Open a new terminal
2. Navigate into the following directory = **RailScope/national_rail_project/local/src/dev/docker/**
3. Run docker compose up
4. Wait until all services get in a healthy status

Once all container are up an running

### 3. Spark application
1. Make sure you have spark installed in your local, if not go to [installation guide](local/README.md)
2. [Submit the spark application](https://spark.apache.org/docs/3.4.0/submitting-applications.html#content)
3. Move into the following directory = **RailScope/national_rail_project/local/src/dev**
4. Run the following program in the same terminal you submitted the spark application
```bash
python spark_main_consumer.py
```
5. Wait until the application starts
6. Go to localhost:8080 to monitor the performance.

Once the spark application it's up and running

### 4. API connnection and message producer
1. Make sure you have a working Network Rail Account, if not go back to the pre-requesite section.
2. Move into the following directory = **RailScope/national_rail_project/local/src/dev**
3. Run the following command
```bash
python message_producer --stomp_username YOUR_EMAIL --stomp_password YOUR_PASSWORD
```
4. If you experience disconections while using this script you can use the following flags to modify the hearbeat interval and reconnection based on your timeout differences.
```bash
--stomp_heartbeat_interval_ms 
--stomp_reconnect_delay_sec
```

Once all of this steps have been accomplished, data will start being ingested into the Kafka clusters and then processed by Spark Structured Streaming, the data will have a trigger time of 15 minutes which means, that data will get uploaded every 15 minutes into our DWH but it gets processed as soon as it comes.

### Project architecture overview

![Project architecture overview](./images/RailScope.png)

The architecture of this project involves several steps to collect, process, and analyze data from UK NationalRail Feed:

1. Using the [StompClient](./local/src/dev/message_producer.py) class, we connect to the UK NationalRail Feed to receive messages every 60 seconds in a JSON format. The class handles the connection, subscription, error handling, and reconnection in case of failure.

2. Before sending the messages to their respective topics, we apply eight [JSON parser functions](./local/src/dev/utils/json_parser.py) to break down the 3,000 lines of nested JSON schema into five dictionaries and three lists of dictionaries. These functions run in a [pool of two workers](./local/src/dev/utils/topic_hub.py) for optimal performance. 

3. Once the desired information is extracted from the API response, the output from the functions is sent to their respective topics.

4. Spark Streaming ingests the data and performs schema enforcement for all incoming messages from the Kafka brokers.

6. After validating the schema, we cast the messages into their proper types.

7. Streaming DataFrames are processed to create aggregations that provide business value to the data.


9. All Streaming DataFrames are loaded into temporal GCS buckets.

10. Data it's automatically loaded from the GCS Bucket into their respective table in BigQuery.

11. The Google Data Studio dashboard is powered by the BigQuery dataset.

This project architecture overview outlines the high-level steps involved in collecting and processing data from UK NationalRail Feed. Each step is designed to ensure the efficient and accurate processing of the data, from extraction to analysis.


## Special Thanks
@iobruno