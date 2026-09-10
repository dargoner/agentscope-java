/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package io.agentscope.extensions.sandbox.opensandbox;

import com.alibaba.opensandbox.sandbox.Sandbox;
import com.alibaba.opensandbox.sandbox.SandboxManager;
import com.alibaba.opensandbox.sandbox.config.ConnectionConfig;
import com.alibaba.opensandbox.sandbox.domain.exceptions.SandboxApiException;
import com.alibaba.opensandbox.sandbox.domain.models.execd.executions.Execution;
import com.alibaba.opensandbox.sandbox.domain.models.execd.executions.OutputMessage;
import com.alibaba.opensandbox.sandbox.domain.models.execd.executions.RunCommandRequest;
import com.alibaba.opensandbox.sandbox.domain.models.execd.filesystem.WriteEntry;
import com.alibaba.opensandbox.sandbox.domain.models.sandboxes.PagedSandboxInfos;
import com.alibaba.opensandbox.sandbox.domain.models.sandboxes.PagedSnapshotInfos;
import com.alibaba.opensandbox.sandbox.domain.models.sandboxes.SandboxFilter;
import com.alibaba.opensandbox.sandbox.domain.models.sandboxes.SandboxInfo;
import com.alibaba.opensandbox.sandbox.domain.models.sandboxes.SnapshotFilter;
import com.alibaba.opensandbox.sandbox.domain.models.sandboxes.SnapshotState;
import io.agentscope.harness.agent.sandbox.ExecResult;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Production bridge to the official OpenSandbox Java SDK. */
final class OfficialOpenSandboxSdk implements OpenSandboxSdk {
    private ConnectionConfig config(OpenSandboxClientOptions options) {
        OpenSandboxEndpoint endpoint = OpenSandboxEndpoint.parse(options.getEndpoint());
        return ConnectionConfig.builder()
                .domain(endpoint.domain())
                .protocol(endpoint.protocol())
                .apiKey(options.getApiKey())
                .requestTimeout(Duration.ofSeconds(options.getRequestTimeoutSeconds()))
                .useServerProxy(options.isUseServerProxy())
                .build();
    }

    @Override
    public Handle create(OpenSandboxState state, OpenSandboxClientOptions options) {
        Sandbox.Builder builder =
                Sandbox.builder()
                        .timeout(Duration.ofSeconds(state.getSandboxTimeoutSeconds()))
                        .readyTimeout(Duration.ofSeconds(options.getReadyTimeoutSeconds()))
                        .connectionConfig(config(options));
        if (state.getRestoreSnapshotId() == null || state.getRestoreSnapshotId().isBlank()) {
            builder.image(state.getImage());
        } else {
            builder.snapshotId(state.getRestoreSnapshotId());
        }
        if (!state.getEntrypoint().isEmpty()) {
            builder.entrypoint(state.getEntrypoint());
        }
        if (!state.getResourceLimits().isEmpty()) {
            builder.resource(state.getResourceLimits());
        }
        if (!state.getMetadata().isEmpty()) {
            builder.metadata(state.getMetadata());
        }
        Sandbox sandbox = builder.build();
        return new HandleImpl(sandbox);
    }

    @Override
    public Handle connect(String sandboxId, OpenSandboxClientOptions options) {
        Sandbox sandbox =
                Sandbox.connector()
                        .sandboxId(sandboxId)
                        .connectionConfig(config(options))
                        .connectTimeout(Duration.ofSeconds(options.getReadyTimeoutSeconds()))
                        .connect();
        return new HandleImpl(sandbox);
    }

    @Override
    public void kill(String sandboxId, OpenSandboxClientOptions options) {
        try (SandboxManager manager =
                SandboxManager.builder().connectionConfig(config(options)).build()) {
            manager.killSandbox(sandboxId);
        }
    }

    @Override
    public OpenSandboxState getInfo(String sandboxId, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            return toState(manager.getSandboxInfo(sandboxId));
        }
    }

    @Override
    public List<OpenSandboxState> listByMetadata(
            Map<String, String> metadata, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            List<OpenSandboxState> states = new ArrayList<>();
            SandboxFilter.Builder filter = SandboxFilter.builder().metadata(metadata).pageSize(100);
            while (true) {
                PagedSandboxInfos page = manager.listSandboxInfos(filter.build());
                page.getSandboxInfos().stream().map(this::toState).forEach(states::add);
                if (!page.getPagination().getHasNextPage()) {
                    return List.copyOf(states);
                }
                filter.page(page.getPagination().getPage() + 1);
            }
        }
    }

    @Override
    public void patchMetadata(
            String sandboxId, Map<String, String> metadata, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            manager.patchSandboxMetadata(sandboxId, metadata);
        }
    }

    @Override
    public Instant renew(String sandboxId, Duration duration, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            return manager.renewSandbox(sandboxId, duration).getExpiresAt().toInstant();
        }
    }

    @Override
    public void pause(String sandboxId, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            manager.pauseSandbox(sandboxId);
        }
    }

    @Override
    public void resumeRemote(String sandboxId, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            manager.resumeSandbox(sandboxId);
        }
    }

    @Override
    public String createSnapshot(String sandboxId, String name, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            return manager.createSnapshot(sandboxId, name).getId();
        }
    }

    @Override
    public String waitForSnapshotReady(
            String snapshotId, Duration readyTimeout, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            return manager.waitForSnapshotReady(snapshotId, readyTimeout).getId();
        }
    }

    @Override
    public Map<String, Instant> listReadySnapshotsByNamePrefix(
            String namePrefix, OpenSandboxClientOptions options) {
        Map<String, Instant> ready = new LinkedHashMap<>();
        listSnapshotDetailsByNamePrefix(namePrefix, options).values().stream()
                .filter(snapshot -> SnapshotState.READY.equalsIgnoreCase(snapshot.status()))
                .forEach(snapshot -> ready.put(snapshot.id(), snapshot.createdAt()));
        return Map.copyOf(ready);
    }

    @Override
    public Map<String, OpenSandboxClient.NativeSnapshot> listSnapshotDetailsByNamePrefix(
            String namePrefix, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            Map<String, OpenSandboxClient.NativeSnapshot> snapshots = new LinkedHashMap<>();
            SnapshotFilter.Builder filter = SnapshotFilter.builder().pageSize(100);
            while (true) {
                PagedSnapshotInfos page = manager.listSnapshots(filter.build());
                page.getSnapshotInfos().stream()
                        .filter(snapshot -> snapshot.getName() != null)
                        .filter(snapshot -> snapshot.getName().startsWith(namePrefix))
                        .forEach(
                                snapshot ->
                                        snapshots.put(
                                                snapshot.getId(),
                                                new OpenSandboxClient.NativeSnapshot(
                                                        snapshot.getId(),
                                                        snapshot.getName(),
                                                        snapshot.getCreatedAt().toInstant(),
                                                        snapshot.getStatus() == null
                                                                ? SnapshotState.UNKNOWN
                                                                : snapshot.getStatus()
                                                                        .getState())));
                if (!page.getPagination().getHasNextPage()) {
                    return Map.copyOf(snapshots);
                }
                filter.page(page.getPagination().getPage() + 1);
            }
        }
    }

    @Override
    public void deleteSnapshot(String snapshotId, OpenSandboxClientOptions options) {
        try (SandboxManager manager = manager(options)) {
            manager.deleteSnapshot(snapshotId);
        }
    }

    @Override
    public boolean isNotFound(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof SandboxApiException apiException
                    && Integer.valueOf(404).equals(apiException.getStatusCode())) {
                return true;
            }
        }
        return false;
    }

    private SandboxManager manager(OpenSandboxClientOptions options) {
        return SandboxManager.builder().connectionConfig(config(options)).build();
    }

    private OpenSandboxState toState(SandboxInfo info) {
        OpenSandboxState state = new OpenSandboxState();
        state.setSandboxId(info.getId());
        state.setSandboxOwned(true);
        state.setEntrypoint(info.getEntrypoint());
        state.setRestoreSnapshotId(info.getSnapshotId());
        state.setMetadata(info.getMetadata());
        state.setRemoteStatus(info.getStatus().getState());
        state.setRemoteCreatedAt(info.getCreatedAt().toInstant());
        state.setRemoteExpiresAt(
                info.getExpiresAt() == null ? null : info.getExpiresAt().toInstant());
        if (info.getImage() != null) {
            state.setImage(info.getImage().getImage());
        }
        return state;
    }

    private static final class HandleImpl implements Handle {
        private final Sandbox sandbox;

        private HandleImpl(Sandbox sandbox) {
            this.sandbox = sandbox;
        }

        @Override
        public String id() {
            return sandbox.getId();
        }

        @Override
        public ExecResult exec(String command, String workingDirectory, int timeoutSeconds) {
            Execution execution =
                    sandbox.commands()
                            .run(
                                    RunCommandRequest.builder()
                                            .command(command)
                                            .workingDirectory(workingDirectory)
                                            .timeout(
                                                    Duration.ofSeconds(Math.max(timeoutSeconds, 1)))
                                            .build());
            String stdout =
                    output(
                            execution == null || execution.getLogs() == null
                                    ? null
                                    : execution.getLogs().getStdout());
            String stderr =
                    output(
                            execution == null || execution.getLogs() == null
                                    ? null
                                    : execution.getLogs().getStderr());
            int exitCode =
                    execution == null || execution.getExitCode() == null
                            ? -1
                            : execution.getExitCode();
            return new ExecResult(exitCode, stdout, stderr, false);
        }

        @Override
        public InputStream read(String absolutePath) {
            return sandbox.files().readStream(absolutePath);
        }

        @Override
        public void write(String absolutePath, byte[] content) {
            sandbox.files()
                    .writeFile(WriteEntry.builder().path(absolutePath).data(content).build());
        }

        @Override
        public void close() {
            sandbox.close();
        }

        private static String output(List<OutputMessage> messages) {
            if (messages == null || messages.isEmpty()) return "";
            StringBuilder result = new StringBuilder();
            for (OutputMessage message : messages) {
                if (message != null && message.getText() != null) result.append(message.getText());
            }
            return result.toString();
        }
    }
}
