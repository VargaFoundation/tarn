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

/**
 * Hardware accelerator types TARN can request from YARN. Each maps to a YARN resource name
 * that the NodeManagers must advertise (via {@code resource-types.xml}) and the scheduler
 * must honour (via {@code DominantResourceCalculator}).
 *
 * <p>Hardcoding {@code yarn.io/gpu} was fine when NVIDIA was the only story, but telco edge
 * clusters now mix CPU-only nodes, AMD MI300s, and Intel Gaudi, and the same AM should be
 * able to orchestrate any of them — the Triton container image picks the matching backend.
 */
public enum AcceleratorType {
    /** NVIDIA CUDA GPUs — the default. */
    NVIDIA_GPU("yarn.io/gpu"),
    /** AMD ROCm GPUs (MI200/MI300). Requires the AMD device plugin on NodeManagers. */
    AMD_GPU("amd.com/gpu"),
    /** Intel Gaudi accelerators. */
    INTEL_GAUDI("habana.ai/gaudi"),
    /** AWS Neuron (Inferentia / Trainium). */
    AWS_NEURON("aws.amazon.com/neuron"),
    /** CPU-only inference; no accelerator resource requested. */
    CPU_ONLY(null);

    private final String yarnResourceName;

    AcceleratorType(String yarnResourceName) {
        this.yarnResourceName = yarnResourceName;
    }

    public boolean requiresAcceleratorResource() {
        return yarnResourceName != null;
    }

    public String yarnResourceName() {
        return yarnResourceName;
    }

    public static AcceleratorType parse(String s) {
        if (s == null || s.isEmpty()) return NVIDIA_GPU;
        try {
            return AcceleratorType.valueOf(s.toUpperCase().replace('-', '_'));
        } catch (IllegalArgumentException e) {
            return NVIDIA_GPU;
        }
    }
}
