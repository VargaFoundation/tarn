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
 * Composite load signal read by {@link ScalingPolicy}.
 *
 * <p>Why a composite and not a scalar: GPU utilization on LLM workloads saturates at 100%
 * and stays there while requests queue up — scaling on GPU% alone makes TARN blind to
 * overload. Queue depth (or its derivative, tail latency) is the true leading indicator.
 * The {@link ScalingMode} picked by the operator decides which component drives decisions.
 */
public final class LoadSignal {

    public enum ScalingMode {
        /** Legacy mode: scale on average GPU utilization. OK for ONNX/PyTorch batch workloads. */
        GPU_UTIL,
        /** Scale on pending-request depth per container. Correct for LLM/continuous-batching. */
        QUEUE_DEPTH,
        /** Take the max of GPU and queue normalization. Safe default. */
        COMPOSITE;

        public static ScalingMode parse(String s) {
            if (s == null || s.isEmpty()) return COMPOSITE;
            try {
                return ScalingMode.valueOf(s.toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return COMPOSITE;
            }
        }
    }

    /** Average GPU utilization across running containers, in [0.0, 1.0]. */
    public final double gpuUtil;
    /** Total pending (queued) inference requests across all containers. */
    public final int queueDepth;
    /** Per-instance p95 latency in ms (unused for scaling today but exposed for future policies). */
    public final double latencyP95Ms;
    /** Number of containers observed when the signal was taken. Zero when the cluster is empty. */
    public final int numContainers;
    /**
     * Soft ceiling of pending requests per container treated as "100% loaded". Sized against
     * the backend batching width — Triton's inflight_fused_batcher typically handles 16–32 in
     * parallel; beyond that the excess is queued and latency climbs.
     */
    public final int queueCapacityPerContainer;

    public LoadSignal(double gpuUtil, int queueDepth, double latencyP95Ms,
                      int numContainers, int queueCapacityPerContainer) {
        this.gpuUtil = clamp(gpuUtil);
        this.queueDepth = Math.max(0, queueDepth);
        this.latencyP95Ms = Math.max(0.0, latencyP95Ms);
        this.numContainers = Math.max(0, numContainers);
        this.queueCapacityPerContainer = Math.max(1, queueCapacityPerContainer);
    }

    /** Normalized load in [0, 1] for the given mode. */
    public double normalizedLoad(ScalingMode mode) {
        switch (mode) {
            case GPU_UTIL:
                return gpuUtil;
            case QUEUE_DEPTH:
                return queueNormalized();
            case COMPOSITE:
            default:
                return Math.max(gpuUtil, queueNormalized());
        }
    }

    private double queueNormalized() {
        if (numContainers == 0) return queueDepth == 0 ? 0.0 : 1.0;
        double perContainer = (double) queueDepth / (double) numContainers;
        return clamp(perContainer / (double) queueCapacityPerContainer);
    }

    private static double clamp(double v) {
        if (Double.isNaN(v)) return 0.0;
        if (v < 0.0) return 0.0;
        if (v > 1.0) return 1.0;
        return v;
    }

    @Override
    public String toString() {
        return "LoadSignal{gpuUtil=" + gpuUtil
                + ", queueDepth=" + queueDepth
                + ", latencyP95Ms=" + latencyP95Ms
                + ", containers=" + numContainers + "}";
    }
}
