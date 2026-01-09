# TARN: Triton on YARN

TARN is a scalable inference solution for running NVIDIA Triton Inference Server on a Hadoop/YARN cluster using Docker containers.

## Architecture

```mermaid
graph TB
    %% Client
    Client[Client]

    %% Load Balancer
    subgraph LoadBalancer ["Load Balancer"]
        HA[HAProxy Instance]
        Updater[HAProxy Updater Script]
    end

    %% YARN Cluster
    subgraph YARN ["YARN Cluster"]
        RM[Resource Manager]

        subgraph NM1 ["Node Manager 1"]
            AM[Application Master]
        end

        subgraph NM2 ["Node Manager 2"]
            TC1[Triton Container 1<br/>GPU Allocated]
            NFS1[HDFS NFS Gateway 1]
        end

        subgraph NM3 ["Node Manager 3"]
            TC2[Triton Container 2<br/>GPU Allocated]
            NFS2[HDFS NFS Gateway 1]
        end

        TC1 -->|3. Load model| NFS1
        TC2 -->|3. Load model| NFS2
    end

    %% HDFS
    subgraph HDFS ["HDFS"]
        NN[NameNode]
        DN[DataNode]
    end

    %% Connexions principales (flux vertical)
    Client -->|7. Inference Request| HA
    HA -->|Load Balances| TC1
    HA -->|Load Balances| TC2

    %% Interactions YARN
    AM -.->|1. Request Resources| RM
    RM -.->|2. Allocate Containers| NM2
    RM -.->|2. Allocate Containers| NM3

    %% Découverte et mise à jour HAProxy
    Updater -.->|4. Discover AM| RM
    Updater -->|5. Query Instances| AM
    Updater -->|6. Update backend servers<br/>list via Runtime API| HA

    %% Accès HDFS via NFS Gateway
    NFS1 -.-> NN
    NFS2 -.-> NN
    NFS1 --> DN
    NFS2 --> DN

    %% Style pour forcer l'empilement vertical
    classDef cluster fill:#2d2d2d,stroke:#444,color:#fff;
    class LoadBalancer,YARN,HDFS cluster;
```

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
- **High Availability**: Support for Application Master restart, recovering active containers from previous attempts.
- **Health Monitoring**: Integrated health checks using Triton's `/v2/health/ready` endpoint.
- **Secret Management**: Support for JKS/JCEKS secret files on HDFS for sensitive data like Hugging Face tokens.
- **Model Storage**: Support for HDFS (with automatic copy) or NFS (direct access via NFS Gateway).
- Support for Open Inference Protocol (OIP).

## Model Repository Storage

TARN supports two ways to provide the model repository to Triton:

### 1. HDFS (Copy to Local)
If your model repository path starts with `hdfs:///`, TARN will automatically copy the models from HDFS to a local directory (`/models`) inside the container before starting Triton.
- **Pros**: Easy to set up, no extra configuration on nodes.
- **Cons**: High latency on startup for large models, consumes local disk space.

### 2. NFS (Direct Access - Recommended)
If your path starts with `/`, TARN will use the path directly. This is the recommended method for performance, as it avoids data copying. However, it requires an **NFS Gateway** to be installed and mounted on all NodeManagers.
- **Pros**: Instant startup, no data duplication.
- **Cons**: Requires NFS infrastructure.

#### How to install HDFS NFS Gateway

To use the NFS method, you must deploy an HDFS NFS Gateway on your DataNodes and mount it locally.

1. **Configure HDFS for NFS**:
   Add the following to `core-site.xml`:
   ```xml
   <property>
     <name>hadoop.proxyuser.hdfs.groups</name>
     <value>*</value>
   </property>
   <property>
     <name>hadoop.proxyuser.hdfs.hosts</name>
     <value>*</value>
   </property>
   ```

2. **Start the NFS Gateway**:
   On each DataNode (or dedicated nodes):
   ```bash
   # Start portmap (requires root)
   hdfs portmap
   # Start nfs3
   hdfs nfs3
   ```

3. **Mount HDFS via NFS**:
   On all NodeManagers:
   ```bash
   mkdir -p /mnt/hdfs
   mount -t nfs -o vers=3,proto=tcp,nolock,noacl <NFS_GATEWAY_HOST>:/ /mnt/hdfs
   ```

Now you can point TARN to your models using the local mount path:
`--model-repository /mnt/hdfs/user/models/my-model`

## Prerequisites

- Hadoop 3.3+ configured with Docker Runtime.
- NVIDIA Drivers and NVIDIA Container Toolkit installed on NodeManagers.
- Java 17 and Maven for building.
- **socat** installed on the HAProxy node for dynamic updates.

## Build

Standard build using default Maven repositories:

```bash
mvn clean package
```

### Building with Enterprise Distributions (ODP by Clemlab, Cloudera...)

If you are using enterprise distributions like **ODP** (Open Source Data Platform) by **Clemlab**, or **Cloudera**, you need to configure specific Maven repositories to fetch the distribution-specific Hadoop binaries.

#### 1. Configure Repositories

Add the ODP public repository to your `pom.xml` or your `~/.m2/settings.xml`:

```xml
<repositories>
    <repository>
        <id>odp-public</id>
        <url>https://repo.opensourcedataplatform.com/repository/maven-public/</url>
    </repository>
</repositories>
```

#### 2. Specify Distribution Version

You should align the `hadoop.version` with your distribution's version. You can do this by overriding the property during the build:

```bash
mvn clean package -Dhadoop.version=3.3.6.1.2.4.0-32
```

#### 3. Testing with Distribution Binaries

To run tests using the distribution-specific dependencies:

```bash
mvn test -Dhadoop.version=3.3.6.1.2.4.0-32
```

## Deployment

To submit the application to YARN:

```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository [model_path] \
  --image [triton_image] \
  --port [triton_port] \
  --metrics-port [metrics_port] \
  --am-port [am_port] \
  --address [bind_address] \
  --token [security_token] \
  --tp [tensor_parallelism] \
  --pp [pipeline_parallelism] \
  --secrets [hdfs_jks_path] \
  --placement-tag [tag] \
  --scale-up [threshold] \
  --scale-down [threshold] \
  --min-instances [count] \
  --max-instances [count] \
  --cooldown [ms] \
  --jar [local_jar_path] \
  --env KEY1=VALUE1 --env KEY2=VALUE2
```

Example:
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///user/models \
  --image nvcr.io/nvidia/tritonserver:24.09-py3 \
  --port 8000 \
  --metrics-port 8002 \
  --am-port 8888 \
  --address 0.0.0.0 \
  --token my-secret-token \
  --tp 2 \
  --pp 1 \
  --secrets hdfs:///user/secrets/hf.jceks \
  --placement-tag nvidia \
  --scale-up 0.8 \
  --scale-down 0.2 \
  --min-instances 2 \
  --max-instances 8 \
  --cooldown 120000 \
  --jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar \
  --env TRITON_LOG_VERBOSE=1
```

## Node Tagging and Placement

TARN uses YARN **Placement Constraints** to ensure optimal distribution of Triton instances. By default, it uses the tag `nvidia` for anti-affinity (at most one container per node).

### How to tag nodes in YARN

To use placement constraints effectively, you may want to tag your nodes. In YARN, this is typically done using **Node Labels** or by configuring the NodeManagers.

#### 1. Using Node Labels (Recommended for GPU isolation)
Node labels allow you to partition the cluster. For example, to label nodes with GPUs:
```bash
# Add the label
yarn rmadmin -replaceLabelsOnNode "node1:8041,GPU node2:8041,GPU"
```
Then, you can configure your queue to use these labels. Note that TARN also explicitly requests `yarn.io/gpu` resources.

#### 2. Anti-Affinity via Placement Constraints
TARN automatically handles anti-affinity. If you use the `--placement-tag` option, TARN will:
1. Tag all its Triton containers with this value.
2. Tell YARN to never place two containers with the same tag on the same host.

This ensures that even if you have multiple GPUs on a single node, a single Triton instance (which can manage multiple GPUs via TP/PP) will occupy that node, preventing resource contention between multiple Triton processes.

## Load Balancing and Service Discovery

The script `scripts/update_haproxy.sh` dynamically discovers the Application Master using the YARN CLI and updates HAProxy configuration via its Runtime API.

### HAProxy Installation and Configuration

To use the dynamic load balancing feature, you need to install and configure HAProxy on a node that has access to the YARN cluster.

#### 1. Installation

On Debian/Ubuntu:
```bash
sudo apt-get update
sudo apt-get install haproxy socat
```

On CentOS/RHEL:
```bash
sudo yum install haproxy socat
```

#### 2. Configuration

Edit `/etc/haproxy/haproxy.cfg` to include the Runtime API and a backend with empty slots.

```haproxy
global
    # Enable the Runtime API via a Unix socket
    stats socket /var/run/haproxy.sock mode 660 level admin
    stats timeout 30s

defaults
    mode http
    timeout connect 5s
    timeout client 50s
    timeout server 50s

frontend triton_frontend
    bind *:80
    default_backend triton_backend

backend triton_backend
    balance roundrobin
    # Define placeholder slots that will be updated by the script.
    # The number of slots should be >= max-instances.
    server triton-1 0.0.0.0:8000 check disabled
    server triton-2 0.0.0.0:8000 check disabled
    server triton-3 0.0.0.0:8000 check disabled
    server triton-4 0.0.0.0:8000 check disabled
    server triton-5 0.0.0.0:8000 check disabled
    server triton-6 0.0.0.0:8000 check disabled
    server triton-7 0.0.0.0:8000 check disabled
    server triton-8 0.0.0.0:8000 check disabled
```

#### 3. Permissions

Ensure the user running the `update_haproxy.sh` script has permissions to write to the HAProxy socket:

```bash
sudo chown root:haproxy /var/run/haproxy.sock
sudo chmod 660 /var/run/haproxy.sock
# Add your user to the haproxy group
sudo usermod -a -G haproxy $USER
```

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

## Usage Examples (Open Inference Protocol)

Below are examples of how to consume the inference service using the **Open Inference Protocol** (via the `tritonclient` Python library). These examples assume you are targeting the HAProxy endpoint or a direct Triton instance.

To use Triton, we need to build a model repository. The Triton Inference Server organizes models in a model repository, which is a structured directory containing models and their configurations.


The structure of the repository as follows:

The repository follows a specific layout:

```
<model-repository-path>/
   <model-name>/
       [config.pbtxt]
       [<output-labels-file> ...]
       [configs/]
           [<custom-config-file> ...]
       <version>/
           <model-definition-file>
       <version>/
           <model-definition-file>
```

- Model Name: Each subdirectory under the repository represents a model.

- config.pbtxt: This optional file defines the model's configuration, including backend, input/output shapes, and data types. If not provided, Triton may auto-generate it for supported backends.

- Version Directories: Each model must have at least one numerically named subdirectory (e.g., 1, 2), representing different versions of the model. Non-numeric directories are ignored.

- Model Definition Files: These are backend-specific files (e.g., model.onnx for ONNX, model.pt for PyTorch, model.plan for TensorRT).

Example:

```
model_repository
|
+-- resnet50
    |
    +-- config.pbtxt
    +-- 1
        |
        +-- model.pt
+-- densenet_onnx
    |
    +-- config.pbtxt
    +-- 1
        |
        +-- model.onnx   
```

The config.pbtxt configuration file is optional. The configuration file is autogenerated by Triton Inference Server if the user doesn’t provide it. The config.pbtxt file specifies key details such as:

- Backend: Defines the framework (e.g., TensorFlow, PyTorch, ONNX).
- Inputs/Outputs: Specifies names, shapes, and data types.
- Batching: Configures maximum batch size and dynamic batching policies.

Example config.pbtxt for an ONNX model:

```
name: "text_detection"
backend: "onnxruntime"
max_batch_size: 256
input [
   {
       name: "input_images:0"
       data_type: TYPE_FP32
       dims: [ -1, -1, -1, 3 ]
   }
]
output [
   {
       name: "feature_fusion/Conv_7/Sigmoid:0"
       data_type: TYPE_FP32
       dims: [ -1, -1, -1, 1 ]
   }
]
```

Triton supports multiple versions of a model within the same repository. Each version resides in its own directory (e.g., 1/, 2/). By default, Triton serves the latest version, but this behavior can be customized using version policies.

### 1. Stable Diffusion (Image Generation)

To deploy Stable Diffusion, you need to have the model repository structured correctly.
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///models \
  --image nvcr.io/nvidia/tritonserver:24.09-py3 \
  --token secret-token
```

**Client Code (Python):**
```python
import numpy as np
from PIL import Image
from tritonclient.http import InferenceServerClient, InferInput

# Connect to the server (HAProxy or direct instance)
client = InferenceServerClient(url="localhost:8000")

prompt = "A futuristic city in the style of cyberpunk"
input_data = np.array([prompt], dtype=object)

# Setup inputs according to the model configuration
inputs = [InferInput("PROMPT", [1], "BYTES")]
inputs[0].set_data_from_numpy(input_data)

# Run inference
response = client.infer("stable_diffusion", inputs)

# Extract and save the generated image
image_data = response.as_numpy("IMAGES")[0]
image = Image.fromarray(image_data.astype(np.uint8))
image.save("generated_image.png")
```

### 2. ONNX Model (Quick Deploy)

Deploying an ONNX model (e.g., ResNet-50):
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///models \
  --image nvcr.io/nvidia/tritonserver:24.09-py3
```

**Client Code (Python):**
```python
import numpy as np
import tritonclient.http as httpclient

client = httpclient.InferenceServerClient(url="localhost:8000")

# Prepare dummy input data (e.g., for ResNet-50)
input_shape = (1, 3, 224, 224)
data = np.random.randn(*input_shape).astype(np.float32)

inputs = [httpclient.InferInput("input_0", input_shape, "FP32")]
inputs[0].set_data_from_numpy(data)

# Request inference
results = client.infer("onnx_resnet50", inputs)

# Get output
output_data = results.as_numpy("output_0")
print(f"Inference result shape: {output_data.shape}")
```

### 3. PyTorch Model (LibTorch)

For PyTorch models, ensure the model is exported as TorchScript and stored in the repository:
```bash
yarn jar target/tarn-orchestrator-0.0.1-SNAPSHOT.jar varga.tarn.yarn.Client \
  --model-repository hdfs:///models \
  --image nvcr.io/nvidia/tritonserver:24.09-py3
```

**Client Code (Python):**
```python
import numpy as np
import tritonclient.http as httpclient

client = httpclient.InferenceServerClient(url="localhost:8000")

# Example input for an image classification model
data = np.random.randn(1, 3, 224, 224).astype(np.float32)

inputs = [httpclient.InferInput("INPUT__0", [1, 3, 224, 224], "FP32")]
inputs[0].set_data_from_numpy(data)

# Call Triton
response = client.infer("pytorch_densenet", inputs)

# Parse output
probabilities = response.as_numpy("OUTPUT__0")
predicted_class = np.argmax(probabilities)
print(f"Predicted class ID: {predicted_class}")
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
