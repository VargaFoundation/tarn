package varga.tarn.yarn;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ScalingPolicyTest {

    @Test
    public void testScaleUp() {
        ScalingPolicy policy = new ScalingPolicy(0.7, 0.2, 1, 10, 0);
        int newTarget = policy.calculateTarget(1, 0.8);
        assertEquals(2, newTarget);
    }

    @Test
    public void testScaleDown() {
        ScalingPolicy policy = new ScalingPolicy(0.7, 0.2, 1, 10, 0);
        int newTarget = policy.calculateTarget(2, 0.1);
        assertEquals(1, newTarget);
    }

    @Test
    public void testNoChange() {
        ScalingPolicy policy = new ScalingPolicy(0.7, 0.2, 1, 10, 0);
        assertEquals(1, policy.calculateTarget(1, 0.5));
        assertEquals(10, policy.calculateTarget(10, 0.9)); // Max reached
        assertEquals(1, policy.calculateTarget(1, 0.1));  // Min reached
    }

    @Test
    public void testCooldown() {
        ScalingPolicy policy = new ScalingPolicy(0.7, 0.2, 1, 10, 1000);
        assertEquals(2, policy.calculateTarget(1, 0.8)); // Scales up
        assertEquals(2, policy.calculateTarget(2, 0.8)); // Cooldown active
    }
}
