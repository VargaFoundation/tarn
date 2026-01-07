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
- Support for Open Inference Protocol (OIP).

## Prerequisites

- Hadoop 3.3+ configured with Docker Runtime.
- NVIDIA Drivers and NVIDIA Container Toolkit installed on NodeManagers.
- Java 17 and Maven for building.

## Build

```bash
mvn clean package
```

## Deployment

To submit the application to YARN:

```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client [hdfs_model_path] [triton_image]
```

Example:
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client hdfs:///user/models/resnet50 nvcr.io/nvidia/tritonserver:24.09-py3
```

## Load Balancing

A script `scripts/update_haproxy.sh` is provided to update HAProxy configuration by querying the Application Master.

```bash
./scripts/update_haproxy.sh <AM_HOST> <HAPROXY_SOCKET_PATH>
```
