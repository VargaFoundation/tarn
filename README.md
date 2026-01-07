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
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository [hdfs_model_path] \
  --image [triton_image] \
  --port [triton_port] \
  --am-port [am_port] \
  --token [security_token]
```

Example:
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///user/models/resnet50 \
  --image nvcr.io/nvidia/tritonserver:24.09-py3 \
  --port 8000 \
  --am-port 8888 \
  --token my-secret-token
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
