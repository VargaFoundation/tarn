package varga.tarn.operator;

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

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * Typed Kubernetes Custom Resource for {@code TritonDeployment}.
 *
 * <p>The CRD schema lives in {@code helm/crds/tritondeployment.yaml}; this class is the
 * Java projection used by the TARN operator and by tests driving fabric8's KubernetesServer
 * mock. The separation is intentional: the YAML is the K8s API contract; this class is a
 * convenience for typed access and is allowed to lag fields it doesn't need.
 */
@Group("tarn.varga.io")
@Version("v1alpha1")
@Kind("TritonDeployment")
@Plural("tritondeployments")
@ShortNames({"td", "triton"})
public class TritonDeployment extends CustomResource<TritonDeploymentSpec, TritonDeploymentStatus>
        implements Namespaced {
}
