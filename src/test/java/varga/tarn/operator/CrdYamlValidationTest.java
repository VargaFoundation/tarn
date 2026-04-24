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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catches regressions where the YAML that ships with the Helm chart stops matching the
 * Java typing. A desync would mean {@code kubectl apply -f example-tritondeployment.yaml}
 * succeeds on the cluster but then the operator's typed parser rejects the CR at runtime,
 * or vice versa.
 *
 * <p>These run without a real Kubernetes cluster — they parse the YAML with fabric8's
 * serializer and walk the JSON tree directly.
 */
public class CrdYamlValidationTest {

    private static final Path CRD_YAML =
            Paths.get("helm/crds/tritondeployment.yaml");
    private static final Path EXAMPLE_CR =
            Paths.get("helm/crds/example-tritondeployment.yaml");

    @Test
    public void crdYamlParsesAsCustomResourceDefinition() throws IOException {
        assertTrue(Files.exists(CRD_YAML), "CRD YAML must ship with the chart");
        String yaml = Files.readString(CRD_YAML);
        CustomResourceDefinition crd = Serialization.unmarshal(yaml, CustomResourceDefinition.class);
        assertNotNull(crd, "YAML must parse into CustomResourceDefinition");
        assertEquals("tarn.varga.io", crd.getSpec().getGroup());
        assertEquals("TritonDeployment", crd.getSpec().getNames().getKind());
        assertEquals(1, crd.getSpec().getVersions().size());
        assertEquals("v1alpha1", crd.getSpec().getVersions().get(0).getName());
        assertTrue(crd.getSpec().getVersions().get(0).getServed());
        assertTrue(crd.getSpec().getVersions().get(0).getStorage());
        // Status subresource must be declared — reconciler writes to it.
        assertNotNull(crd.getSpec().getVersions().get(0).getSubresources(),
                "subresources block required");
        assertNotNull(crd.getSpec().getVersions().get(0).getSubresources().getStatus(),
                "status subresource must be enabled");
    }

    @Test
    public void crdSchemaRequiresSpecFieldsThatJavaAlsoRequires() throws IOException {
        // The Java reconciler validates that image and modelRepository are set, refusing
        // otherwise. The CRD should enforce the same at apiserver level so bad CRs never
        // reach the operator at all — this guards the reconciler from malformed inputs.
        String yaml = Files.readString(CRD_YAML);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode root = mapper.readTree(yaml);
        JsonNode specProps = root.path("spec").path("versions").get(0)
                .path("schema").path("openAPIV3Schema")
                .path("properties").path("spec");
        assertTrue(specProps.path("required").isArray(),
                "spec.required must be an array");
        assertTrue(listContains(specProps.path("required"), "image"),
                "CRD must mark 'image' required");
        assertTrue(listContains(specProps.path("required"), "modelRepository"),
                "CRD must mark 'modelRepository' required");
    }

    @Test
    public void exampleCrParsesIntoTypedJavaClass() throws IOException {
        assertTrue(Files.exists(EXAMPLE_CR), "example CR must ship with the chart");
        String yaml = Files.readString(EXAMPLE_CR);
        TritonDeployment cr = Serialization.unmarshal(yaml, TritonDeployment.class);
        assertNotNull(cr);
        assertEquals("llama-3-70b", cr.getMetadata().getName());
        assertNotNull(cr.getSpec());
        assertEquals("nvcr.io/nvidia/tritonserver:24.09-py3", cr.getSpec().getImage());
        assertEquals(3, cr.getSpec().getReplicas());
        assertEquals(2, cr.getSpec().getTensorParallelism());
        assertNotNull(cr.getSpec().getAccelerator());
        assertEquals("nvidia_gpu", cr.getSpec().getAccelerator().getType());
        assertNotNull(cr.getSpec().getOpenaiProxy());
        assertTrue(cr.getSpec().getOpenaiProxy().getEnabled());
        assertEquals(9000, cr.getSpec().getOpenaiProxy().getPort());
        assertEquals("composite", cr.getSpec().getScaling().getMode());
        assertEquals("triton_prod", cr.getSpec().getRanger().getServiceName());
        assertNotNull(cr.getSpec().getEnv());
        assertEquals(1, cr.getSpec().getEnv().size());
    }

    @Test
    public void crdAcceleratorEnumMatchesJavaMapping() throws IOException {
        // The enum values in the CRD must be a subset of what AcceleratorType.kubernetesResourceName
        // knows about. If someone adds a new enum value in YAML without updating Java,
        // reconciliation returns the wrong resource name — this test catches that.
        String yaml = Files.readString(CRD_YAML);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode enumNode = mapper.readTree(yaml)
                .path("spec").path("versions").get(0)
                .path("schema").path("openAPIV3Schema")
                .path("properties").path("spec").path("properties")
                .path("accelerator").path("properties").path("type").path("enum");
        assertTrue(enumNode.isArray());
        for (JsonNode v : enumNode) {
            String type = v.asText();
            TritonDeploymentSpec.Accelerator a = new TritonDeploymentSpec.Accelerator();
            a.setType(type);
            // Either it returns a resource name OR it's cpu_only which returns null.
            // Anything else would throw — test would fail.
            String resource = a.kubernetesResourceName();
            if ("cpu_only".equals(type)) {
                assertNull(resource, "cpu_only must not request an accelerator resource");
            } else {
                assertNotNull(resource, "enum value '" + type + "' must map to a K8s resource");
            }
        }
    }

    private static boolean listContains(JsonNode arr, String value) {
        if (!arr.isArray()) return false;
        for (JsonNode n : arr) if (value.equals(n.asText())) return true;
        return false;
    }
}
