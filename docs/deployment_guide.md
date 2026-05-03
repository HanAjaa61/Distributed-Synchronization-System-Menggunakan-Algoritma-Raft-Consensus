# Deployment Guide

## Overview

This guide explains how to deploy and run the Distributed Sync System locally or using Docker. The system is built using Python and implements a distributed architecture with RAFT consensus.

---

## Requirements

Before starting, make sure you have:

* Python 3.10 or higher
* pip (Python package manager)
* virtualenv (optional but recommended)
* Docker & Docker Compose (optional for containerized deployment)

---

## Project Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd distributed-sync-system
```

---

### 2. Create Virtual Environment

#### Windows:

```bash
python -m venv venv
venv\Scripts\activate
```

#### Linux / Mac:

```bash
python3 -m venv venv
source venv/bin/activate
```

---

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Setup Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

For Windows (PowerShell):

```powershell
copy .env.example .env
```

Edit `.env` if needed:

```env
APP_PORT=8000
NODE_ID=node1
CLUSTER_NODES=node1,node2,node3
ELECTION_TIMEOUT=150
HEARTBEAT_INTERVAL=50
DEBUG=True
```

---

## Running the Application

### Run Single Node

```bash
python main.py
```

The API will be available at:

```
http://localhost:8000
```

---

### Run Multiple Nodes (Manual Simulation)

Open multiple terminals and run:

Terminal 1:

```bash
set NODE_ID=node1
python main.py
```

Terminal 2:

```bash
set NODE_ID=node2
python main.py
```

Terminal 3:

```bash
set NODE_ID=node3
python main.py
```

---

## Docker Deployment (Recommended)

### 1. Build Docker Image

```bash
docker build -f docker/Dockerfile.node -t distributed-sync-node .
```

---

### 2. Run Using Docker Compose

```bash
docker-compose -f docker/docker-compose.yml up
```

---

### 3. Stop Containers

```bash
docker-compose down
```

---

## Testing

### Run Unit Tests

```bash
pytest
```

---

### Run Performance Tests

```bash
pytest test/performance
```

---

## API Access

Common endpoints:

* Health Check:

```
GET /health
```

* Sync Data:

```
POST /sync
```

* RAFT Status:

```
GET /raft/status
```

---

## Troubleshooting

### Port Already in Use

Change port in `.env`:

```env
APP_PORT=8001
```

---

### Module Not Found Error

Make sure virtual environment is activated:

```bash
venv\Scripts\activate
```

---

### Docker Issues

Ensure Docker is running:

```bash
docker --version
```

---

## Notes

* Make sure all nodes are configured correctly in `CLUSTER_NODES`
* Use Docker for easier multi-node simulation
* Logs can be monitored via terminal output

---

## Conclusion

This deployment setup allows you to:

* Run a single-node system for development
* Simulate a distributed system locally
* Deploy using Docker for scalability

The system is ready for testing and further development.
