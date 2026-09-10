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
package io.agentscope.extensions.sandbox.opensandbox.redis;

import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClientOptions;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.filesystem.spec.SandboxFilesystemSpec;
import io.agentscope.harness.agent.sandbox.SandboxClient;
import io.agentscope.harness.agent.sandbox.SandboxException;
import io.agentscope.harness.agent.sandbox.SandboxExecutionGuard;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.snapshot.NoopSnapshotSpec;
import io.agentscope.harness.agent.sandbox.snapshot.SandboxSnapshotSpec;
import java.util.List;
import java.util.Objects;
import org.redisson.api.RedissonClient;

/** Fluent sandbox filesystem configuration for Redis-coordinated OpenSandbox workspaces. */
public class RedisOpenSandboxFilesystemSpec extends SandboxFilesystemSpec {

    private final RedissonClient redisson;
    private OpenSandboxClientOptions options = new OpenSandboxClientOptions();
    private OpenSandboxRedisLifecycleOptions lifecycle = new OpenSandboxRedisLifecycleOptions();
    private WorkspaceSpec defaultWorkspaceSpec = new WorkspaceSpec();
    private SandboxSnapshotSpec snapshotSpec = new NoopSnapshotSpec();
    private SandboxClient<?> clientOverride;
    private SandboxClient<?> ownedClient;

    /**
     * Creates a filesystem spec backed by an application-owned Redisson client.
     *
     * @param redisson externally managed Redisson client
     */
    public RedisOpenSandboxFilesystemSpec(RedissonClient redisson) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        super.isolationScope(IsolationScope.USER);
        super.snapshotSpec(snapshotSpec);
        super.executionGuard(SandboxExecutionGuard.noop());
    }

    public RedisOpenSandboxFilesystemSpec clientOptions(OpenSandboxClientOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        return this;
    }

    public RedisOpenSandboxFilesystemSpec lifecycleOptions(
            OpenSandboxRedisLifecycleOptions lifecycle) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        return this;
    }

    public RedisOpenSandboxFilesystemSpec workspaceSpec(WorkspaceSpec workspaceSpec) {
        this.defaultWorkspaceSpec = Objects.requireNonNull(workspaceSpec, "workspaceSpec");
        return this;
    }

    public RedisOpenSandboxFilesystemSpec client(SandboxClient<?> client) {
        this.clientOverride = client;
        this.ownedClient = null;
        return this;
    }

    @Override
    public RedisOpenSandboxFilesystemSpec workspaceProjectionEnabled(boolean enabled) {
        super.workspaceProjectionEnabled(enabled);
        return this;
    }

    @Override
    public RedisOpenSandboxFilesystemSpec workspaceProjectionRoots(List<String> includeRoots) {
        super.workspaceProjectionRoots(includeRoots);
        return this;
    }

    @Override
    public RedisOpenSandboxFilesystemSpec executionGuard(SandboxExecutionGuard executionGuard) {
        if (executionGuard != null && executionGuard != SandboxExecutionGuard.noop()) {
            throw new SandboxException.SandboxConfigurationException(
                    "Redis OpenSandbox coordinates lifecycle with leases; an execution guard"
                            + " across the full turn is unsupported");
        }
        super.executionGuard(SandboxExecutionGuard.noop());
        return this;
    }

    @Override
    public RedisOpenSandboxFilesystemSpec isolationScope(IsolationScope scope) {
        super.isolationScope(Objects.requireNonNull(scope, "scope"));
        return this;
    }

    @Override
    public RedisOpenSandboxFilesystemSpec snapshotSpec(SandboxSnapshotSpec snapshotSpec) {
        rejectTarSnapshot(snapshotSpec);
        this.snapshotSpec = snapshotSpec != null ? snapshotSpec : new NoopSnapshotSpec();
        super.snapshotSpec(this.snapshotSpec);
        return this;
    }

    @Override
    protected SandboxClient<?> createClient() {
        if (clientOverride != null) {
            return clientOverride;
        }
        ownedClient = new RedisOpenSandboxClient(redisson, options, lifecycle);
        return ownedClient;
    }

    @Override
    protected boolean isClientOwned(SandboxClient<?> client) {
        return client == ownedClient;
    }

    @Override
    protected OpenSandboxClientOptions clientOptions() {
        return options;
    }

    @Override
    protected SandboxSnapshotSpec snapshotSpec() {
        return snapshotSpec;
    }

    @Override
    protected WorkspaceSpec workspaceSpec() {
        return defaultWorkspaceSpec;
    }

    private static void rejectTarSnapshot(SandboxSnapshotSpec snapshotSpec) {
        if (snapshotSpec != null && !(snapshotSpec instanceof NoopSnapshotSpec)) {
            throw new SandboxException.SandboxConfigurationException(
                    "Redis OpenSandbox uses native snapshots; tar snapshot specs are unsupported");
        }
    }
}
