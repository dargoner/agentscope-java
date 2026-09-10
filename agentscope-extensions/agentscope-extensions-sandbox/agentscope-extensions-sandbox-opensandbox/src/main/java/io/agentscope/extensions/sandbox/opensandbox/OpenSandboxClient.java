/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package io.agentscope.extensions.sandbox.opensandbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxClient;
import io.agentscope.harness.agent.sandbox.SandboxException;
import io.agentscope.harness.agent.sandbox.SandboxState;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.json.HarnessSandboxJacksonModule;
import io.agentscope.harness.agent.sandbox.snapshot.SandboxSnapshotSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** {@link SandboxClient} backed by OpenSandbox. */
public class OpenSandboxClient implements SandboxClient<OpenSandboxClientOptions> {
    public record NativeSnapshot(String id, String name, Instant createdAt, String status) {
        public NativeSnapshot(String id, String name, Instant createdAt) {
            this(id, name, createdAt, "READY");
        }

        public NativeSnapshot {
            requireText(id, "id");
            requireText(name, "name");
            Objects.requireNonNull(createdAt, "createdAt");
            requireText(status, "status");
        }
    }

    private final ObjectMapper objectMapper;
    private final OpenSandboxClientOptions defaultOptions;
    private final OpenSandboxSdk sdk;

    public OpenSandboxClient() {
        this(new OpenSandboxClientOptions(), null);
    }

    public OpenSandboxClient(OpenSandboxClientOptions defaultOptions, ObjectMapper objectMapper) {
        this(defaultOptions, objectMapper, new OfficialOpenSandboxSdk());
    }

    OpenSandboxClient(
            OpenSandboxClientOptions defaultOptions,
            ObjectMapper objectMapper,
            OpenSandboxSdk sdk) {
        this.defaultOptions =
                defaultOptions != null
                        ? OpenSandboxClientOptions.copyOf(defaultOptions)
                        : new OpenSandboxClientOptions();
        this.objectMapper =
                objectMapper != null
                        ? objectMapper
                        : new ObjectMapper()
                                .findAndRegisterModules()
                                .registerModule(new HarnessSandboxJacksonModule())
                                .registerModule(new OpenSandboxHarnessSandboxJacksonModule());
        this.sdk = sdk != null ? sdk : new OfficialOpenSandboxSdk();
    }

    @Override
    public Sandbox create(
            WorkspaceSpec workspaceSpec,
            SandboxSnapshotSpec snapshotSpec,
            OpenSandboxClientOptions options) {
        OpenSandboxClientOptions merged = merge(options);
        OpenSandboxState state = new OpenSandboxState();
        state.setSessionId(UUID.randomUUID().toString());
        state.setWorkspaceSpec(workspaceSpec != null ? workspaceSpec.copy() : new WorkspaceSpec());
        state.setImage(merged.getImage());
        state.setEntrypoint(merged.getEntrypoint());
        state.setResourceLimits(merged.getResourceLimits());
        state.setRestoreSnapshotId(merged.getRestoreSnapshotId());
        state.setMetadata(merged.getMetadata());
        state.setSandboxTimeoutSeconds(merged.getSandboxTimeoutSeconds());
        state.setSandboxOwned(true);
        state.setWorkspaceRootReady(false);
        if (snapshotSpec != null) {
            state.setSnapshot(snapshotSpec.build(state.getSessionId()));
        }
        return new OpenSandbox(state, merged, sdk);
    }

    @Override
    public Sandbox resume(SandboxState state) {
        if (!(state instanceof OpenSandboxState openSandboxState)) {
            throw new IllegalArgumentException(
                    "Expected OpenSandboxState but got "
                            + (state == null ? "null" : state.getClass().getName()));
        }
        return new OpenSandbox(
                openSandboxState, OpenSandboxClientOptions.copyOf(defaultOptions), sdk);
    }

    @Override
    public void delete(Sandbox sandbox) {
        if (sandbox != null) {
            try {
                sandbox.shutdown();
            } catch (Exception e) {
                throw new SandboxException.SandboxRuntimeException(
                        "Failed to delete OpenSandbox", e);
            }
        }
    }

    @Override
    public String serializeState(SandboxState state) {
        try {
            return objectMapper.writeValueAsString(state);
        } catch (Exception e) {
            throw new SandboxException.SandboxConfigurationException(
                    "Failed to serialize OpenSandbox state", e);
        }
    }

    @Override
    public SandboxState deserializeState(String json) {
        try {
            return objectMapper.readValue(json, SandboxState.class);
        } catch (Exception e) {
            throw new SandboxException.SandboxConfigurationException(
                    "Failed to deserialize OpenSandbox state", e);
        }
    }

    public OpenSandboxState describe(String sandboxId) {
        return invoke(() -> sdk.getInfo(requireText(sandboxId, "sandboxId"), options()));
    }

    public List<OpenSandboxState> listByMetadata(Map<String, String> metadata) {
        return invoke(
                () -> sdk.listByMetadata(Map.copyOf(Objects.requireNonNull(metadata)), options()));
    }

    public void patchMetadata(String sandboxId, Map<String, String> metadata) {
        invoke(
                () -> {
                    sdk.patchMetadata(
                            requireText(sandboxId, "sandboxId"),
                            Map.copyOf(Objects.requireNonNull(metadata)),
                            options());
                    return null;
                });
    }

    public Instant renew(String sandboxId, Duration duration) {
        return invoke(
                () ->
                        sdk.renew(
                                requireText(sandboxId, "sandboxId"),
                                requirePositive(duration, "duration"),
                                options()));
    }

    public void pause(String sandboxId) {
        invoke(
                () -> {
                    sdk.pause(requireText(sandboxId, "sandboxId"), options());
                    return null;
                });
    }

    public void resumeRemote(String sandboxId) {
        invoke(
                () -> {
                    sdk.resumeRemote(requireText(sandboxId, "sandboxId"), options());
                    return null;
                });
    }

    public String createNativeSnapshot(String sandboxId, String name) {
        return invoke(
                () ->
                        sdk.createSnapshot(
                                requireText(sandboxId, "sandboxId"),
                                requireText(name, "name"),
                                options()));
    }

    public String createNativeSnapshot(String sandboxId, String name, Duration readyTimeout) {
        return waitForNativeSnapshotReady(
                createNativeSnapshot(sandboxId, name),
                requirePositive(readyTimeout, "readyTimeout"));
    }

    public String waitForNativeSnapshotReady(String snapshotId, Duration readyTimeout) {
        return invoke(
                () ->
                        sdk.waitForSnapshotReady(
                                requireText(snapshotId, "snapshotId"),
                                requirePositive(readyTimeout, "readyTimeout"),
                                options()));
    }

    public Map<String, Instant> listReadyNativeSnapshotsByNamePrefix(String namePrefix) {
        return invoke(
                () ->
                        sdk.listReadySnapshotsByNamePrefix(
                                requireText(namePrefix, "namePrefix"), options()));
    }

    public Map<String, NativeSnapshot> listNativeSnapshotDetailsByNamePrefix(String namePrefix) {
        return invoke(
                () ->
                        sdk.listSnapshotDetailsByNamePrefix(
                                requireText(namePrefix, "namePrefix"), options()));
    }

    public void deleteNativeSnapshot(String snapshotId) {
        invoke(
                () -> {
                    sdk.deleteSnapshot(requireText(snapshotId, "snapshotId"), options());
                    return null;
                });
    }

    public boolean isNotFound(Throwable error) {
        return sdk.isNotFound(error);
    }

    private OpenSandboxClientOptions merge(OpenSandboxClientOptions call) {
        OpenSandboxClientOptions merged = OpenSandboxClientOptions.copyOf(defaultOptions);
        if (call == null) {
            return merged;
        }
        merged.setEndpoint(call.getEndpoint());
        if (call.getApiKey() != null) merged.setApiKey(call.getApiKey());
        merged.setImage(call.getImage());
        merged.setEntrypoint(call.getEntrypoint());
        merged.setResourceLimits(call.getResourceLimits());
        merged.setRestoreSnapshotId(call.getRestoreSnapshotId());
        merged.setMetadata(call.getMetadata());
        merged.setSandboxTimeoutSeconds(call.getSandboxTimeoutSeconds());
        merged.setReadyTimeoutSeconds(call.getReadyTimeoutSeconds());
        merged.setRequestTimeoutSeconds(call.getRequestTimeoutSeconds());
        merged.setUseServerProxy(call.isUseServerProxy());
        return merged;
    }

    private OpenSandboxClientOptions options() {
        return OpenSandboxClientOptions.copyOf(defaultOptions);
    }

    private <T> T invoke(CheckedSupplier<T> operation) {
        try {
            return operation.get();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new SandboxException.SandboxRuntimeException(
                    "OpenSandbox management operation failed", e);
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    @FunctionalInterface
    private interface CheckedSupplier<T> {
        T get() throws Exception;
    }
}
