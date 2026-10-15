# Cloud Workload Platform

Distributed workload execution platform featuring live compute worker nodes, job queuing, stateful transaction ledger, and real-time performance telemetry.

---

## Prerequisites

- **Docker** and **Docker Compose** installed and running on your system.

---

## Running with Docker Compose

The project runs as a 3-node distributed cluster using [docker-compose.yml](file:///d:/cloud_package/docker-compose.yml):
- **`cloud-gateway-node`**: Cloud Control Plane & Ingress Dashboard (`http://localhost:5000`)
- **`cloud-worker-node-01`**: Distributed Compute Worker Node A (`http://localhost:5001`)
- **`cloud-worker-node-02`**: Distributed Compute Worker Node B (`http://localhost:5002`)

### 1. Build and Start the Cluster

To build the images and start all services in the foreground:
```bash
docker compose up --build
```

To run in the background (detached mode):
```bash
docker compose up -d --build
```

### 2. View Service Status and Logs

Check running containers:
```bash
docker compose ps
```

View live logs from all containers:
```bash
docker compose logs -f
```

View logs for a specific service (e.g. gateway):
```bash
docker compose logs -f cloud-gateway-node
```

### 3. Access the Dashboard and APIs

Once the cluster is running:
- **Web Dashboard**: [http://localhost:5000](http://localhost:5000)
- **Cluster Stats API**: `GET http://localhost:5000/api/stats`
- **Submit Job API**: `POST http://localhost:5000/api/submit_job`

### 4. Stop the Cluster

To stop and remove containers and network:
```bash
docker compose down
```
