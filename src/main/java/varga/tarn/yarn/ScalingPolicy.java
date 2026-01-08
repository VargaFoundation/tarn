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
