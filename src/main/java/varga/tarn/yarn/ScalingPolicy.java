/*
 * Copyright © 2008 Varga Foundation (contact@varga.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package varga.tarn.yarn;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScalingPolicy {
    private static final Logger log = LoggerFactory.getLogger(ScalingPolicy.class);

    private final double upThreshold;
    private final double downThreshold;
    private final int maxContainers;
    private final int minContainers;
    private final long cooldownMs;
    private long lastScaleTime = 0;

    public ScalingPolicy() {
        this(0.7, 0.2, 1, 10, 60000);
    }

    public ScalingPolicy(double upThreshold, double downThreshold, int minContainers, int maxContainers, long cooldownMs) {
        this.upThreshold = upThreshold;
        this.downThreshold = downThreshold;
        this.minContainers = minContainers;
        this.maxContainers = maxContainers;
        this.cooldownMs = cooldownMs;
    }

    public int calculateTarget(int currentTarget, double avgLoad) {
        long now = System.currentTimeMillis();
        if (now - lastScaleTime < cooldownMs) {
            return currentTarget;
        }

        int newTarget = currentTarget;
        if (avgLoad > upThreshold && currentTarget < maxContainers) {
            log.info("High load detected ({}), scaling up...", String.format("%.2f", avgLoad));
            newTarget = currentTarget + 1;
        } else if (avgLoad < downThreshold && currentTarget > minContainers) {
            log.info("Low load detected ({}), scaling down...", String.format("%.2f", avgLoad));
            newTarget = currentTarget - 1;
        }

        if (newTarget != currentTarget) {
            lastScaleTime = now;
        }
        return newTarget;
    }
}
