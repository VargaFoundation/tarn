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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScalingPolicy {
    private static final Logger log = LoggerFactory.getLogger(ScalingPolicy.class);

    private final double upThreshold;
    private final double downThreshold;
    private final int maxContainers;
    private final int minContainers;
    private final long cooldownMs;
    private final LoadSignal.ScalingMode mode;
    private long lastScaleTime = 0;

    public ScalingPolicy() {
        this(0.7, 0.2, 1, 10, 60000);
    }

    public ScalingPolicy(double upThreshold, double downThreshold, int minContainers, int maxContainers, long cooldownMs) {
        this(upThreshold, downThreshold, minContainers, maxContainers, cooldownMs, LoadSignal.ScalingMode.COMPOSITE);
    }

    public ScalingPolicy(double upThreshold, double downThreshold, int minContainers, int maxContainers,
                         long cooldownMs, LoadSignal.ScalingMode mode) {
        this.upThreshold = upThreshold;
        this.downThreshold = downThreshold;
        this.minContainers = minContainers;
        this.maxContainers = maxContainers;
        this.cooldownMs = cooldownMs;
        this.mode = mode == null ? LoadSignal.ScalingMode.COMPOSITE : mode;
    }

    public LoadSignal.ScalingMode getMode() {
        return mode;
    }

    /**
     * Legacy scalar entry point preserved so pre-P1.5 callers and ScalingPolicyTest continue
     * to function. Treats the scalar as the normalized load regardless of mode.
     */
    public int calculateTarget(int currentTarget, double avgLoad) {
        return decide(currentTarget, avgLoad, "load=" + String.format("%.2f", avgLoad));
    }

    /** Preferred entry point: a composite signal + mode-aware normalization. */
    public int calculateTarget(int currentTarget, LoadSignal signal) {
        double load = signal.normalizedLoad(mode);
        return decide(currentTarget, load, "mode=" + mode + " " + signal + " -> load=" + String.format("%.2f", load));
    }

    private int decide(int currentTarget, double load, String context) {
        long now = System.currentTimeMillis();
        if (now - lastScaleTime < cooldownMs) {
            return currentTarget;
        }

        int newTarget = currentTarget;
        if (load > upThreshold && currentTarget < maxContainers) {
            log.info("Scaling up (ctx: {})", context);
            newTarget = currentTarget + 1;
        } else if (load < downThreshold && currentTarget > minContainers) {
            log.info("Scaling down (ctx: {})", context);
            newTarget = currentTarget - 1;
        }

        if (newTarget != currentTarget) {
            lastScaleTime = now;
        }
        return newTarget;
    }
}
