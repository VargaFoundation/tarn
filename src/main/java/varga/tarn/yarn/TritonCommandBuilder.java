package varga.tarn.yarn;

/*-
 * #%L
 * Tarn
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import java.util.regex.Pattern;

public class TritonCommandBuilder {
    // Rejects any shell metacharacter. Paths must start with hdfs:// or /.
    private static final Pattern UNSAFE_PATH_CHARS = Pattern.compile("[\\s;&|`$()<>\"'\\\\*?]");
    private static final Pattern SAFE_BIND_ADDRESS = Pattern.compile("^[a-zA-Z0-9.\\-:]+$");

    private String modelRepository;
    private int httpPort = 8000;
    private int grpcPort = 8001;
    private int metricsPort = 8002;
    private String bindAddress = "0.0.0.0";
    private int tp = 1;
    private int pp = 1;
    private String secretsPath;

    public static void requireSafePath(String label, String value) {
        if (value == null || value.isEmpty()) return;
        if (UNSAFE_PATH_CHARS.matcher(value).find()) {
            throw new IllegalArgumentException(
                    "Invalid " + label + ": contains unsafe shell characters (" + value + ")");
        }
        if (!value.startsWith("hdfs://") && !value.startsWith("/")) {
            throw new IllegalArgumentException(
                    "Invalid " + label + ": must start with 'hdfs://' or '/' (" + value + ")");
        }
    }

    public TritonCommandBuilder modelRepository(String modelRepository) {
        requireSafePath("model-repository", modelRepository);
        this.modelRepository = modelRepository;
        return this;
    }

    public TritonCommandBuilder httpPort(int httpPort) {
        this.httpPort = httpPort;
        return this;
    }

    public TritonCommandBuilder grpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
        return this;
    }

    public TritonCommandBuilder metricsPort(int metricsPort) {
        this.metricsPort = metricsPort;
        return this;
    }

    public TritonCommandBuilder bindAddress(String bindAddress) {
        if (bindAddress != null && !SAFE_BIND_ADDRESS.matcher(bindAddress).matches()) {
            throw new IllegalArgumentException(
                    "Invalid bind-address: only alphanumerics, dot, dash and colon allowed (" + bindAddress + ")");
        }
        this.bindAddress = bindAddress;
        return this;
    }

    public TritonCommandBuilder tensorParallelism(int tp) {
        this.tp = tp;
        return this;
    }

    public TritonCommandBuilder pipelineParallelism(int pp) {
        this.pp = pp;
        return this;
    }

    public TritonCommandBuilder secretsPath(String secretsPath) {
        requireSafePath("secrets", secretsPath);
        this.secretsPath = secretsPath;
        return this;
    }

    public String build() {
        // Defense-in-depth: re-validate in case fields were set reflectively or via no-arg ctor.
        requireSafePath("model-repository", modelRepository);
        requireSafePath("secrets", secretsPath);

        StringBuilder sb = new StringBuilder();
        String localModelPath = "/models";

        // For HDFS paths the AM registers each model file as a YARN LocalResource (see
        // ApplicationMaster.buildHdfsModelLocalResources). With YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS=true,
        // those files are mounted into the docker container at the working directory, so the
        // model tree shows up at ./models — no in-container `hadoop fs` step (which would fail
        // since the Triton image has no hadoop CLI).
        if (modelRepository != null && !modelRepository.isEmpty()) {
            if (modelRepository.startsWith("hdfs://")) {
                localModelPath = "./models";
            } else if (modelRepository.startsWith("/")) {
                localModelPath = modelRepository;
            }
        }

        // Secrets logic
        if (secretsPath != null && !secretsPath.isEmpty()) {
            sb.append("mkdir -p /secrets && ");
            sb.append("hadoop fs -copyToLocal ").append(secretsPath).append(" /secrets/secrets.jks && ");
        }

        int worldSize = tp * pp;
        if (worldSize > 1) {
            sb.append("mpirun --allow-run-as-root ");
            for (int i = 0; i < worldSize; i++) {
                if (i > 0) sb.append(" : ");
                sb.append("-n 1 tritonserver ");
                sb.append("--id rank").append(i).append(" ");
                sb.append("--model-repository ").append(localModelPath).append(" ");
                sb.append("--backend-config python,shm-region-prefix-name=rank").append(i).append("_ ");

                if (i == 0) {
                    sb.append(getCommonArgs(httpPort, grpcPort, metricsPort, bindAddress));
                } else {
                    sb.append("--http-port ").append(httpPort + i * 10).append(" ");
                    sb.append("--grpc-port ").append(grpcPort + i * 10).append(" ");
                    sb.append("--allow-http false --allow-grpc false --allow-metrics false ");
                    sb.append("--log-info false --log-warning false --model-control-mode explicit --load-model tensorrt_llm ");
                    sb.append("--model-load-thread-count 2 ");
                }
            }
        } else {
            sb.append("tritonserver ");
            sb.append("--model-repository ").append(localModelPath).append(" ");
            sb.append(getCommonArgs(httpPort, grpcPort, metricsPort, bindAddress));
        }

        return sb.toString().trim();
    }

    private String getCommonArgs(int httpPort, int grpcPort, int metricsPort, String bindAddress) {
        return String.format("--http-port %d --grpc-port %d --metrics-port %d " +
                        "--http-address %s --metrics-address %s " +
                        "--allow-cpu-metrics false --allow-gpu-metrics false --allow-metrics true " +
                        "--metrics-interval-ms 1000 --model-load-thread-count 2 --strict-readiness true ",
                httpPort, grpcPort, metricsPort, bindAddress, bindAddress);
    }
}
