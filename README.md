# 🌌 Financial AI & Agentic Drift Platform

**A distributed, production-grade microservices architecture featuring an autonomous financial agent, real-time social networking, and pioneering "Agentic Drift" telemetry.**

[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)](https://www.docker.com/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-Streaming-E6522C)](https://flink.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-On%20Demand-009688)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-Frontend-61DAFB)](https://reactjs.org/)

[View Architecture](#architecture) • [Core Features](#core-features) • [Tech Stack](#tech-stack) • [Getting Started](#getting-started)
</div>

---

## 💡 The Hook: Why This Project Exists

In the era of Autonomous AI, deploying an agent is easy—but **monitoring its behavioral degradation in production is incredibly hard.** 

This project goes beyond a standard "chat with a document" app. It is a full-scale, distributed system built to mimic the infrastructure of giants like Facebook or Discord, combined with cutting-edge MLOps. 

It features an on-demand **Financial Analyst Agent** powered by LangGraph, but the true innovation lies in its **Agentic Drift Telemetry System**. Every reasoning step, tool invocation, and state mutation the agent makes is captured, streamed, and analyzed to detect "hallucination loops", API over-reliance, and behavioral drift in real-time.

---

## 🏗️ Architecture Overview

<img width="1301" height="713" alt="Screenshot 2026-09-08 at 1 28 48 AM" src="https://github.com/user-attachments/assets/d11d126f-e6a3-4bb5-ad9e-a418261af50e" />
<div align="center">

---

## ✨ Core Innovations

### 1. 🤖 Agentic Drift Telemetry (The MLOps Differentiator)
* **What it does:** Tracks how the AI agent's behavior changes over time compared to a baseline.
* **How it works:** A custom `DriftTelemetryWrapper` intercepts LangGraph execution sequences and streams them to a JSONL log. 
* **The Dashboard:** A beautiful, grid-based Streamlit dashboard calculates dynamic Z-scores to immediately alert engineers if the agent is stuck in reasoning loops or overusing expensive APIs (like Tavily Search or yfinance).

### 2. ⚡ Real-Time Personalization & Social Feed
* **What it does:** Delivers a personalized financial news feed dynamically.
* **How it works:** Utilizes a Node.js Event Server tied to Kafka for Change Data Capture (CDC). As users interact with the app, Apache Flink streams process the events, applying Jaccard LSH (Locality-Sensitive Hashing) to calculate similarity matrices and recommend content with ultra-low latency.

### 3. 💬 High-Scale Chat Architecture
* **What it does:** Enables one-to-one and one-to-many messaging, inspired by Discord and WhatsApp.
* **How it works:** Built on a dedicated Node.js Chat Server utilizing WebSockets, Redis for Pub/Sub and session state, and MongoDB for horizontally scalable message storage.

---

## 🛠️ Technology Stack

| Domain | Technologies |
| :--- | :--- |
| **Agentic AI & MLOps** | Python, LangGraph, LangChain, FastAPI, Streamlit, Pandas |
| **Backend & APIs** | Node.js, Express.js, WebSockets |
| **Streaming & Big Data** | Apache Kafka, Apache Flink, Apache Spark |
| **Databases & Caching** | MongoDB, Redis |
| **Frontend** | React, Vite, Tailwind CSS / Vanilla CSS |
| **DevOps** | Docker, Docker Compose |

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- API Keys for the AI Agent (Tavily, Anthropic, Gemini)

### Quickstart

1. **Clone the repository**
   ```bash
   git clone https://github.com/adarsh4567/project-2.git
   cd project-2
   ```

2. **Environment Variables**
   Set up your `.env` file at the root with your API keys.

3. **Launch the Infrastructure**
   ```bash
   # Spin up the core databases, brokers, and backends
   docker compose up -d
   ```

4. **Access the Services**
   - **Frontend App:** `http://localhost:5173`
   - **Agentic Drift Dashboard:** `http://localhost:8501`
   - **FastAPI Agent Docs:** `http://localhost:8000/docs`

---

## 📈 Future Improvements

See [`docs/improvement.md`](docs/improvement.md) for the roadmap, including scaling out the chat server partitions and optimizing the Spark jobs for massive data ingestion.

---
<div align="center">
  <i>Built to demonstrate production-ready System Design, Big Data Streaming, and cutting-edge MLOps.</i>
</div>
