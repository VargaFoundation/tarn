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

public class LoadSignalTest {

    @Test
    public void gpuModeIgnoresQueueDepth() {
        // On LLM workloads GPU stays at 100% and the queue grows — GPU_UTIL mode returns gpu only.
        LoadSignal s = new LoadSignal(1.0, 200, 0.0, 2, 16);
        assertEquals(1.0, s.normalizedLoad(LoadSignal.ScalingMode.GPU_UTIL));
    }

    @Test
    public void queueModeIgnoresGpuAtSteadyState() {
        // GPU 100% but only 4 pending per container of 16 cap -> queue load = 0.25
        LoadSignal s = new LoadSignal(1.0, 8, 0.0, 2, 16);
        assertEquals(0.25, s.normalizedLoad(LoadSignal.ScalingMode.QUEUE_DEPTH), 1e-9);
    }

    @Test
    public void compositeTakesMax() {
        LoadSignal s = new LoadSignal(0.3, 24, 0.0, 2, 16); // queue/container = 12, norm = 0.75
        assertEquals(0.75, s.normalizedLoad(LoadSignal.ScalingMode.COMPOSITE), 1e-9);
    }

    @Test
    public void queueDepthClampedTo1() {
        LoadSignal s = new LoadSignal(0.0, 1000, 0.0, 2, 16);
        assertEquals(1.0, s.normalizedLoad(LoadSignal.ScalingMode.QUEUE_DEPTH));
    }

    @Test
    public void emptyClusterWithQueueReportsOverload() {
        // Before the first container lands: any pending work is "overload" — ensures scale-up.
        LoadSignal s = new LoadSignal(0.0, 5, 0.0, 0, 16);
        assertEquals(1.0, s.normalizedLoad(LoadSignal.ScalingMode.QUEUE_DEPTH));
    }

    @Test
    public void parseModeFallsBackToComposite() {
        assertEquals(LoadSignal.ScalingMode.COMPOSITE, LoadSignal.ScalingMode.parse(null));
        assertEquals(LoadSignal.ScalingMode.COMPOSITE, LoadSignal.ScalingMode.parse(""));
        assertEquals(LoadSignal.ScalingMode.COMPOSITE, LoadSignal.ScalingMode.parse("unknown"));
        assertEquals(LoadSignal.ScalingMode.QUEUE_DEPTH, LoadSignal.ScalingMode.parse("queue_depth"));
        assertEquals(LoadSignal.ScalingMode.QUEUE_DEPTH, LoadSignal.ScalingMode.parse("queue-depth"));
        assertEquals(LoadSignal.ScalingMode.GPU_UTIL, LoadSignal.ScalingMode.parse("gpu_util"));
    }
}
