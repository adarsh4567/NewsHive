<div align="center">
  
# 🌌 NewsHive - Collaborative News Discovery

### A production-grade distributed system featuring an autonomous Financial Analyst Agent, real-time social networking, and a pioneering **Agentic Drift Telemetry** engine for MLOps observability.

<br/>

[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-Stream%20Processing-E6522C?logo=apacheflink&logoColor=white)](https://flink.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-Agent%20Backend-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-Frontend-61DAFB?logo=react&logoColor=white)](https://reactjs.org/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Agentic%20AI-FF6B35)](https://www.langchain.com/langgraph)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)

[Architecture](#️-architecture) • [Core Innovations](#-core-innovations) • [Tech Stack](#️-technology-stack) • [Getting Started](#-getting-started) • [Roadmap](#-roadmap)

</div>

---

## 💡 The Problem This Solves

Deploying an AI agent is easy. **Knowing when it starts to break — silently — is incredibly hard.**

Most production AI systems have zero observability into *how* an agent reasons. They can't detect when a model enters a hallucination loop, starts over-relying on expensive external API calls, or subtly drifts from its original behavior baseline — until it's too late.

This platform addresses that gap. It's a full-scale distributed system modeled after real-world infrastructure at companies like Discord or Bloomberg, combining:

- A fully autonomous **Financial Analyst Agent** powered by LangGraph
- A **Discord/WhatsApp-inspired chat system** with WebSockets and Redis Pub/Sub
- A **real-time personalization engine** using Kafka, Apache Flink, and Jaccard LSH
- And at its core: an **Agentic Drift Telemetry System** — a custom MLOps layer that monitors, scores, and alerts on AI behavioral degradation in real time

---

## 🏗️ Architecture

The platform runs as a fully containerized microservices stack orchestrated by Docker Compose.

<p align="center">
  <img width="1301" height="713" alt="System Architecture Diagram" src="https://github.com/user-attachments/assets/d11d126f-e6a3-4bb5-ad9e-a418261af50e" />
</p>

<details>
<summary><strong>Service Breakdown</strong></summary>

| Service | Role |
|---|---|
| **FastAPI Agent Server** | Hosts the LangGraph financial analyst agent; exposes REST endpoints |
| **Node.js Event Server** | Captures user interactions; emits CDC events to Kafka |
| **Node.js Chat Server** | WebSocket-based real-time messaging with Redis Pub/Sub |
| **Apache Kafka** | Central event broker for all inter-service communication |
| **Apache Flink** | Stream processor for personalization and LSH similarity scoring |
| **Apache Spark** | Batch analytics and data ingestion |
| **MongoDB** | Horizontally scalable storage for messages and user data |
| **Redis** | In-memory caching, session state, and Pub/Sub for chat |
| **Streamlit Dashboard** | Real-time Agentic Drift visualization and alerting |
| **React + Vite Frontend** | User-facing app with personalized feed and chat UI |

</details>

---

## ✨ Core Innovations

### 1. 🔬 Agentic Drift Telemetry — The MLOps Differentiator

> *"You can't improve what you can't measure."*

This is the flagship feature of the platform. While other AI projects simply call an LLM, this system **instruments the agent's internal reasoning** to detect behavioral drift before it impacts users.

**How it works:**
- A custom `DriftTelemetryWrapper` wraps LangGraph's execution graph, intercepting every reasoning step, tool invocation, and state mutation
- Each event is streamed to a structured JSONL telemetry log with timestamps and execution metadata
- A Streamlit dashboard computes **dynamic Z-scores** against a behavioral baseline, triggering alerts when the agent deviates — catching issues like:
  - 🔁 **Hallucination loops** — repeated reasoning steps with no forward progress
  - 💸 **API over-reliance** — excessive calls to expensive tools like Tavily Search or yfinance
  - 📉 **Behavioral drift** — statistically significant deviation from the agent's expected behavior profile

**Why it matters:** This is the kind of observability layer that separates a demo project from a production-ready AI system.

---

### 2. ⚡ Real-Time Personalization Engine

A recommendation pipeline delivering personalized financial news with sub-second latency.

**How it works:**
- User interactions are captured by a Node.js Event Server and published to **Apache Kafka** via Change Data Capture (CDC)
- **Apache Flink** consumes the stream and applies **Jaccard LSH (Locality-Sensitive Hashing)** to compute content similarity matrices on the fly
- The resulting recommendations are pushed back to the frontend in real time — no page reload, no batch delay

**The challenge it solves:** Traditional recommendation engines run batch jobs on hourly or daily cycles. This pipeline reacts to user behavior within milliseconds.

---

### 3. 💬 High-Scale Chat Architecture

A production-grade messaging system inspired by Discord and WhatsApp, supporting both 1:1 and broadcast messaging.

**How it works:**
- A dedicated **Node.js Chat Server** manages persistent WebSocket connections for real-time message delivery
- **Redis Pub/Sub** handles fan-out across server instances, enabling horizontal scaling without sticky sessions
- **MongoDB** provides horizontally scalable, schema-flexible message storage with efficient range queries on conversation history

---

## 🛠️ Technology Stack

| Domain | Technologies |
|---|---|
| **Agentic AI & MLOps** | Python, LangGraph, LangChain, FastAPI, Streamlit, Pandas |
| **Backend & APIs** | Node.js, Express.js, WebSockets |
| **Event Streaming & Big Data** | Apache Kafka, Apache Flink, Apache Spark |
| **Databases & Caching** | MongoDB, Redis |
| **Frontend** | React, Vite, Tailwind CSS |
| **DevOps & Infra** | Docker, Docker Compose |
| **External AI APIs** | Anthropic Claude, Google Gemini, Tavily Search, yfinance |

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & Docker Compose
- API keys for: **Anthropic**, **Gemini**, and **Tavily**

### Quickstart

```bash
# 1. Clone the repository
git clone https://github.com/adarsh4567/project-2.git
cd project-2

# 2. Configure environment variables
cp .env.example .env
# Fill in your API keys in .env

# 3. Launch all services
docker compose up -d
```

### Service Endpoints

| Service | URL |
|---|---|
| 🖥️ **Frontend App** | http://localhost:5173 |
| 🔬 **Agentic Drift Dashboard** | http://localhost:8501 |
| 📄 **FastAPI Agent Docs** | http://localhost:8000/docs |

### Environment Variables

```env
ANTHROPIC_API_KEY=your_anthropic_key
GEMINI_API_KEY=your_gemini_key
TAVILY_API_KEY=your_tavily_key
```

---

## 📁 Project Structure

```
project-2/
├── agent/                  # LangGraph Financial Analyst Agent (FastAPI)
│   ├── drift_telemetry/    # DriftTelemetryWrapper & JSONL logger
│   └── dashboard/          # Streamlit drift monitoring dashboard
├── chat-server/            # Node.js WebSocket chat backend
├── event-server/           # Node.js Kafka CDC event producer
├── flink-jobs/             # Apache Flink stream processing jobs (LSH)
├── spark-jobs/             # Apache Spark batch analytics
├── frontend/               # React + Vite user interface
├── docs/                   # Architecture diagrams and improvement roadmap
└── docker-compose.yml      # Full stack orchestration
```

---

## 📈 Roadmap

See [`docs/improvement.md`](docs/improvement.md) for the full roadmap. Highlights include:

- [ ] Horizontal scaling of the chat server with partition-aware Kafka consumers
- [ ] Optimizing Spark ingestion jobs for high-volume financial data
- [ ] Expanding drift metrics to include token-level cost tracking
- [ ] Adding a replay mechanism to re-run agent sessions for debugging

---

## 🔗 Links

- 📊 **Architecture Presentation:** [ai-news-app-rho.vercel.app](https://ai-news-app-rho.vercel.app)
- 📖 **Improvement Roadmap:** [`docs/improvement.md`](docs/improvement.md)

---

<div align="center">
  <sub>Built to demonstrate production-ready System Design, Big Data Streaming, and MLOps-grade AI observability.</sub>
</div>
