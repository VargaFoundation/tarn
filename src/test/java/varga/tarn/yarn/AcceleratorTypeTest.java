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

import static org.junit.jupiter.api.Assertions.*;

public class AcceleratorTypeTest {

    @Test
    public void parseFallsBackToNvidia() {
        // Default when unspecified preserves the pre-P2.6 behaviour.
        assertEquals(AcceleratorType.NVIDIA_GPU, AcceleratorType.parse(null));
        assertEquals(AcceleratorType.NVIDIA_GPU, AcceleratorType.parse(""));
        assertEquals(AcceleratorType.NVIDIA_GPU, AcceleratorType.parse("unknown-brand"));
    }

    @Test
    public void parseHandlesAliases() {
        assertEquals(AcceleratorType.AMD_GPU, AcceleratorType.parse("amd-gpu"));
        assertEquals(AcceleratorType.AMD_GPU, AcceleratorType.parse("amd_gpu"));
        assertEquals(AcceleratorType.INTEL_GAUDI, AcceleratorType.parse("intel-gaudi"));
        assertEquals(AcceleratorType.AWS_NEURON, AcceleratorType.parse("aws-neuron"));
        assertEquals(AcceleratorType.CPU_ONLY, AcceleratorType.parse("cpu-only"));
    }

    @Test
    public void cpuOnlyRequestsNoAcceleratorResource() {
        assertFalse(AcceleratorType.CPU_ONLY.requiresAcceleratorResource());
        assertNull(AcceleratorType.CPU_ONLY.yarnResourceName());
    }

    @Test
    public void acceleratorResourceNamesMatchConvention() {
        assertEquals("yarn.io/gpu", AcceleratorType.NVIDIA_GPU.yarnResourceName());
        assertEquals("amd.com/gpu", AcceleratorType.AMD_GPU.yarnResourceName());
        assertEquals("habana.ai/gaudi", AcceleratorType.INTEL_GAUDI.yarnResourceName());
        assertEquals("aws.amazon.com/neuron", AcceleratorType.AWS_NEURON.yarnResourceName());
    }
}
