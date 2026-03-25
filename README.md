# 🚀 Real-Time Data Pipeline using Kafka, Flink & Elasticsearch

## 📌 Overview

This project simulates a real-time user interaction system and processes streaming data using Apache Flink. The processed data is stored in Elasticsearch and visualized using Kibana dashboards.

---

## 🏗️ Architecture

Kafka Producer → Kafka Topic → Flink Consumer → Elasticsearch → Kibana Dashboard

---

## ⚙️ Tech Stack

* Apache Kafka (Data Streaming)
* Apache Flink (Real-time Processing)
* Elasticsearch (NoSQL Storage)
* Kibana (Dashboard Visualization)
* Java

---

## 🔄 Features

### ✅ Real-time Data Generation

* Simulates user interactions (click, view, purchase)

### ✅ Streaming Processing (Flink)

* 5-second window aggregations
* 1-minute window aggregations
* Min / Max / Avg calculations
* Total event count
* Throughput (events/sec)

### ✅ Smart Alerts

* Detects high activity items

### ✅ Elasticsearch Integration

* Stores processed results for analytics

### ✅ Kibana Dashboard

* Multiple charts in one view
* Real-time refresh enabled
* Top active items visualization
* Throughput monitoring

---

## 📊 Dashboard Preview

(Add screenshots here)

---

## ▶️ How to Run

### 1. Start Services

* Start Zookeeper
* Start Kafka
* Start Elasticsearch
* Start Kibana

### 2. Run Kafka Producer

```
Run KafkaProducerApp.java
```

### 3. Run Flink Job

```
Run FlinkJob.java
```

### 4. Open Dashboard

```
http://localhost:5601
```

---

## 📈 Key Metrics

* Events per second (Throughput)
* Top active items
* Min/Max values
* Total event count

---

## 💡 Use Cases

* Real-time analytics
* E-commerce tracking
* User behavior monitoring
* Fraud detection systems

---

## 👨‍💻 Author

Your Name

---
