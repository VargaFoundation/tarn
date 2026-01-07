package varga.tarn.yarn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScalingPolicy {
    private static final Logger log = LoggerFactory.getLogger(ScalingPolicy.class);
    
    private final double upThreshold = 0.7;
    private final double downThreshold = 0.2;
    private final int maxContainers = 10;
    private final int minContainers = 1;

    public int calculateTarget(int currentTarget, double avgLoad) {
        if (avgLoad > upThreshold && currentTarget < maxContainers) {
            log.info("High load detected ({}), scaling up...", String.format("%.2f", avgLoad));
            return currentTarget + 1;
        } else if (avgLoad < downThreshold && currentTarget > minContainers) {
            log.info("Low load detected ({}), scaling down...", String.format("%.2f", avgLoad));
            return currentTarget - 1;
        }
        return currentTarget;
    }
}
