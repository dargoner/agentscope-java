/*
 * Copyright 2024-2026 the original author or authors.
 *
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
 */
package io.agentscope.extensions.sandbox.opensandbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.SandboxState;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpenSandboxClientTest {

    @Test
    void createCopiesMergedCreationSettingsIntoState() {
        OpenSandboxClientOptions defaults = new OpenSandboxClientOptions();
        defaults.setImage("ubuntu:24.04");
        defaults.setResourceLimits(Map.of("cpu", "2", "memory", "4Gi"));
        RecordingSdk sdk = new RecordingSdk();
        OpenSandboxClient client = new OpenSandboxClient(defaults, null, sdk);

        OpenSandbox sandbox = (OpenSandbox) client.create(workspace("/workspace"), null, null);
        OpenSandboxState state = (OpenSandboxState) sandbox.getState();

        assertEquals("ubuntu:24.04", state.getImage());
        assertEquals(Map.of("cpu", "2", "memory", "4Gi"), state.getResourceLimits());
        assertTrue(state.isSandboxOwned());
    }

    @Test
    void resumeRejectsAnotherProviderState() {
        OpenSandboxClient client = new OpenSandboxClient();

        assertThrows(IllegalArgumentException.class, () -> client.resume(new SandboxState() {}));
    }

    @Test
    void managementOperationsDelegateToSdkBoundary() {
        OpenSandboxClientOptions defaults = new OpenSandboxClientOptions();
        defaults.setEndpoint("http://opensandbox.internal:8090");
        RecordingSdk sdk = new RecordingSdk();
        OpenSandboxClient client = new OpenSandboxClient(defaults, null, sdk);

        client.pause("sandbox-1");
        client.resumeRemote("sandbox-1");
        Instant renewed = client.renew("sandbox-1", Duration.ofMinutes(30));
        String snapshotId =
                client.createNativeSnapshot("sandbox-1", "workspace-v1", Duration.ofMinutes(10));
        client.patchMetadata("sandbox-1", Map.of("workspace", "one"));
        client.deleteNativeSnapshot("snapshot-old");

        assertEquals(List.of("sandbox-1"), sdk.pausedIds);
        assertEquals(List.of("sandbox-1"), sdk.resumedIds);
        assertEquals(Instant.parse("2026-08-08T10:30:00Z"), renewed);
        assertEquals("snapshot-1", snapshotId);
        assertEquals("sandbox-1", sdk.snapshotSandboxId);
        assertEquals("workspace-v1", sdk.snapshotName);
        assertEquals("snapshot-1", sdk.waitedSnapshotId);
        assertEquals(Duration.ofMinutes(10), sdk.snapshotReadyTimeout);
        assertEquals(Map.of("workspace", "one"), sdk.patchedMetadata);
        assertEquals(List.of("snapshot-old"), sdk.deletedSnapshotIds);
        assertEquals("http://opensandbox.internal:8090", sdk.lastOptions.getEndpoint());
    }

    @Test
    void queryOperationsReturnSdkResultsAndPreserveSnapshotCreatedAt() {
        RecordingSdk sdk = new RecordingSdk();
        OpenSandboxState described = new OpenSandboxState();
        described.setSandboxId("sandbox-1");
        OpenSandboxState discovered = new OpenSandboxState();
        discovered.setSandboxId("sandbox-2");
        sdk.described = described;
        sdk.discovered = List.of(discovered);
        sdk.readySnapshots.put("snapshot-2", Instant.parse("2026-08-08T09:00:00Z"));
        sdk.snapshotDetails.put(
                "snapshot-2",
                new OpenSandboxClient.NativeSnapshot(
                        "snapshot-2",
                        "agentscope-ws-one-2-1000",
                        Instant.parse("2026-08-08T09:00:00Z"),
                        "CREATING"));
        OpenSandboxClient client = new OpenSandboxClient(new OpenSandboxClientOptions(), null, sdk);

        assertEquals(described, client.describe("sandbox-1"));
        assertEquals(List.of(discovered), client.listByMetadata(Map.of("workspace", "one")));
        assertEquals(
                sdk.readySnapshots,
                client.listReadyNativeSnapshotsByNamePrefix("agentscope-ws-one-"));
        assertEquals(
                sdk.snapshotDetails,
                client.listNativeSnapshotDetailsByNamePrefix("agentscope-ws-one-"));
        assertTrue(client.isNotFound(new IllegalStateException("missing")));
    }

    private static WorkspaceSpec workspace(String root) {
        WorkspaceSpec spec = new WorkspaceSpec();
        spec.setRoot(root);
        return spec;
    }

    private static final class RecordingSdk implements OpenSandboxSdk {
        private final List<String> pausedIds = new ArrayList<>();
        private final List<String> resumedIds = new ArrayList<>();
        private final List<String> deletedSnapshotIds = new ArrayList<>();
        private final Map<String, Instant> readySnapshots = new LinkedHashMap<>();
        private final Map<String, OpenSandboxClient.NativeSnapshot> snapshotDetails =
                new LinkedHashMap<>();
        private OpenSandboxState described;
        private List<OpenSandboxState> discovered = List.of();
        private Map<String, String> patchedMetadata;
        private OpenSandboxClientOptions lastOptions;
        private String snapshotSandboxId;
        private String snapshotName;
        private String waitedSnapshotId;
        private Duration snapshotReadyTimeout;

        @Override
        public Handle create(OpenSandboxState state, OpenSandboxClientOptions options) {
            return new Handle() {
                @Override
                public String id() {
                    return "sandbox-created";
                }

                @Override
                public ExecResult exec(
                        String command, String workingDirectory, int timeoutSeconds) {
                    return new ExecResult(0, "", "", false);
                }

                @Override
                public InputStream read(String absolutePath) {
                    return new ByteArrayInputStream(new byte[0]);
                }

                @Override
                public void write(String absolutePath, byte[] content) {}

                @Override
                public void close() {}
            };
        }

        @Override
        public Handle connect(String sandboxId, OpenSandboxClientOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void kill(String sandboxId, OpenSandboxClientOptions options) {}

        @Override
        public OpenSandboxState getInfo(String sandboxId, OpenSandboxClientOptions options) {
            lastOptions = options;
            return described;
        }

        @Override
        public List<OpenSandboxState> listByMetadata(
                Map<String, String> metadata, OpenSandboxClientOptions options) {
            lastOptions = options;
            return discovered;
        }

        @Override
        public void patchMetadata(
                String sandboxId, Map<String, String> metadata, OpenSandboxClientOptions options) {
            lastOptions = options;
            patchedMetadata = metadata;
        }

        @Override
        public Instant renew(
                String sandboxId, Duration duration, OpenSandboxClientOptions options) {
            lastOptions = options;
            return Instant.parse("2026-08-08T10:30:00Z");
        }

        @Override
        public void pause(String sandboxId, OpenSandboxClientOptions options) {
            lastOptions = options;
            pausedIds.add(sandboxId);
        }

        @Override
        public void resumeRemote(String sandboxId, OpenSandboxClientOptions options) {
            lastOptions = options;
            resumedIds.add(sandboxId);
        }

        @Override
        public String createSnapshot(
                String sandboxId, String name, OpenSandboxClientOptions options) {
            lastOptions = options;
            snapshotSandboxId = sandboxId;
            snapshotName = name;
            return "snapshot-1";
        }

        @Override
        public String waitForSnapshotReady(
                String snapshotId, Duration readyTimeout, OpenSandboxClientOptions options) {
            lastOptions = options;
            waitedSnapshotId = snapshotId;
            snapshotReadyTimeout = readyTimeout;
            return snapshotId;
        }

        @Override
        public Map<String, Instant> listReadySnapshotsByNamePrefix(
                String namePrefix, OpenSandboxClientOptions options) {
            lastOptions = options;
            return readySnapshots;
        }

        @Override
        public Map<String, OpenSandboxClient.NativeSnapshot> listSnapshotDetailsByNamePrefix(
                String namePrefix, OpenSandboxClientOptions options) {
            lastOptions = options;
            return snapshotDetails;
        }

        @Override
        public void deleteSnapshot(String snapshotId, OpenSandboxClientOptions options) {
            lastOptions = options;
            deletedSnapshotIds.add(snapshotId);
        }

        @Override
        public boolean isNotFound(Throwable error) {
            return error instanceof IllegalStateException;
        }
    }
}
