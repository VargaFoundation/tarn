# TARN: Triton on YARN

TARN is a scalable inference solution for running NVIDIA Triton Inference Server on a Hadoop/YARN cluster using Docker containers.

## Architecture

The architecture is based on a native YARN application consisting of:
- **YARN Client**: Submits the application to the cluster.
- **Application Master (AM)**: Manages the lifecycle of Triton containers, handles auto-scaling, and exposes a list of active instances.
- **Triton Containers**: Docker instances running Triton Inference Server.
- **HAProxy**: Entry point for clients, dynamically updated via its Runtime API.

## Features

- Native YARN orchestration.
- Docker support for Triton.
- Horizontal auto-scaling managed by the Application Master.
- Local model loading from HDFS (pre-download before Triton starts).
- Service discovery for HAProxy.
- **Monitoring Dashboard**: Web UI exposed by the AM to monitor cluster status, containers, and models (available at `http://AM_HOST:AM_PORT/dashboard`).
- **Prometheus Metrics**: Aggregated metrics endpoint for Grafana (available at `http://AM_HOST:AM_PORT/metrics`).
- **Distributed Inference**: Support for multi-GPU inference using Tensor Parallelism (TP) and Pipeline Parallelism (PP).
- **Anti-Affinity**: Ensures that YARN places at most one Triton container per node for optimal performance and isolation.
- **Secret Management**: Support for JKS/JCEKS secret files on HDFS for sensitive data like Hugging Face tokens.
- Support for Open Inference Protocol (OIP).

## Prerequisites

- Hadoop 3.3+ configured with Docker Runtime.
- NVIDIA Drivers and NVIDIA Container Toolkit installed on NodeManagers.
- Java 17 and Maven for building.
- **socat** installed on the HAProxy node for dynamic updates.

## Build

```bash
mvn clean package
```

## Deployment

To submit the application to YARN:

```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository [hdfs_model_path] \
  --image [triton_image] \
  --port [triton_port] \
  --metrics-port [metrics_port] \
  --am-port [am_port] \
  --address [bind_address] \
  --token [security_token] \
  --tp [tensor_parallelism] \
  --pp [pipeline_parallelism] \
  --secrets [hdfs_jks_path]
```

Example:
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///user/models/llama-3-8b \
  --image nvcr.io/nvidia/tritonserver:24.09-py3 \
  --port 8000 \
  --metrics-port 8002 \
  --am-port 8888 \
  --address 0.0.0.0 \
  --token my-secret-token \
  --tp 2 \
  --pp 1 \
  --secrets hdfs:///user/secrets/hf.jceks
```

## Load Balancing and Service Discovery

The script `scripts/update_haproxy.sh` dynamically discovers the Application Master using the YARN CLI and updates HAProxy configuration via its Runtime API.

### Usage

```bash
./scripts/update_haproxy.sh <HAPROXY_SOCKET_PATH> <SECURITY_TOKEN>
```

Example:
```bash
./scripts/update_haproxy.sh /var/run/haproxy.sock my-secret-token
```

### Running as a Service

A systemd unit file is provided in `services/tarn-haproxy-updater.service`. To install it:
1. Copy the script to `/usr/local/bin/update_haproxy.sh`.
2. Copy the service file to `/etc/systemd/system/`.
3. Update the `ExecStart` and `User` in the service file if necessary.
4. Run `systemctl daemon-reload && systemctl enable --now tarn-haproxy-updater`.

## Security

TARN implements several security features:
- **API Token**: The Application Master can be configured with a token to secure the service discovery endpoint.
- **Dynamic Discovery**: Eliminates the need to hardcode IP addresses, reducing exposure.
- **YARN Isolation**: Leverages YARN's multi-tenancy and Docker isolation.
- **Kerberos Support**: Compatible with secured Hadoop clusters (ensure the discovery script has a valid ticket).

## Deployment Examples

### 1. Stable Diffusion (Image Generation)
To deploy Stable Diffusion, you need to have the model repository structured correctly on HDFS.
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///models/stable-diffusion \
  --image nvcr.io/nvidia/tritonserver:24.09-py3 \
  --token secret-token
```
Ensure you have the Python backend dependencies installed in your custom Docker image if using the standard one isn't enough.

### 2. ONNX Model (Quick Deploy)
Deploying an ONNX model (e.g., ResNet-50) is straightforward:
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///models/onnx_resnet50 \
  --image nvcr.io/nvidia/tritonserver:24.09-py3
```

### 3. PyTorch Model
For PyTorch models (LibTorch), ensure the `model.pt` is in the correct version folder:
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///models/pytorch_densenet \
  --image nvcr.io/nvidia/tritonserver:24.09-py3
```

## Monitoring with Grafana

You can connect Grafana to the Application Master by adding a Prometheus data source pointing to:
`http://<AM_HOST>:<AM_PORT>/metrics?token=<YOUR_TOKEN>`

Available metrics:
- `tarn_target_containers`: Target number of containers for scaling.
- `tarn_running_containers`: Actual number of running containers.
- `tarn_container_load`: Per-container load based on GPU or request activity.
- `tarn_gpu_utilization`: Per-GPU utilization.
- `tarn_gpu_memory_used`: Per-GPU memory usage.
