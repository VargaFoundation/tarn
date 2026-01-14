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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
