/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandbox;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxFileTransfer;
import io.agentscope.harness.agent.sandbox.SandboxState;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** Per-Turn handle for a shared Redis-managed OpenSandbox workspace. */
public final class RedisManagedOpenSandbox implements Sandbox, SandboxFileTransfer {
    private final RedisOpenSandboxClient owner;
    private final OpenSandbox delegate;
    private final OpenSandboxActiveLease lease;
    private final Object heartbeatMonitor = new Object();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean released = new AtomicBoolean();
    private final AtomicBoolean releaseComplete = new AtomicBoolean();
    private volatile ScheduledFuture<?> heartbeat;

    RedisManagedOpenSandbox(
            RedisOpenSandboxClient owner, OpenSandbox delegate, OpenSandboxActiveLease lease) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lease = Objects.requireNonNull(lease, "lease");
    }

    @Override
    public void start() throws Exception {
        if (released.get()) {
            throw new IllegalStateException("Redis-managed OpenSandbox has already been released");
        }
        if (running.compareAndSet(false, true)) {
            try {
                synchronized (heartbeatMonitor) {
                    if (released.get()) {
                        throw new IllegalStateException(
                                "Redis-managed OpenSandbox has already been released");
                    }
                    heartbeat =
                            owner.startHeartbeat(lease, heartbeatMonitor, () -> !released.get());
                }
            } catch (Exception error) {
                running.set(false);
                try {
                    stop();
                } catch (Exception cleanup) {
                    error.addSuppressed(cleanup);
                }
                throw error;
            }
        }
    }

    @Override
    public synchronized void stop() throws Exception {
        if (releaseComplete.get()) {
            return;
        }
        running.set(false);
        Exception failure = null;
        if (released.compareAndSet(false, true)) {
            synchronized (heartbeatMonitor) {
                ScheduledFuture<?> currentHeartbeat = heartbeat;
                heartbeat = null;
                if (currentHeartbeat != null) {
                    try {
                        currentHeartbeat.cancel(false);
                    } catch (RuntimeException error) {
                        failure = error;
                    }
                }
            }
            try {
                delegate.disconnect();
            } catch (Exception error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        try {
            owner.release(this);
            releaseComplete.set(true);
        } catch (Exception error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void shutdown() {
        // Remote lifecycle is owned by RedisOpenSandboxClient.delete and the idle sweeper.
    }

    @Override
    public void close() throws Exception {
        stop();
        shutdown();
    }

    @Override
    public boolean isRunning() {
        return running.get() && !released.get();
    }

    @Override
    public String getWorkspaceRoot() {
        return delegate.getWorkspaceRoot();
    }

    @Override
    public SandboxState getState() {
        return delegate.getState();
    }

    @Override
    public ExecResult exec(RuntimeContext runtimeContext, String command, Integer timeoutSeconds)
            throws Exception {
        return delegate.exec(runtimeContext, command, timeoutSeconds);
    }

    @Override
    public InputStream persistWorkspace() throws Exception {
        return delegate.persistWorkspace();
    }

    @Override
    public void hydrateWorkspace(InputStream archive) throws Exception {
        delegate.hydrateWorkspace(archive);
    }

    @Override
    public boolean supportsFileTransfer(String absolutePath) {
        return delegate.supportsFileTransfer(absolutePath);
    }

    @Override
    public void uploadFile(String absolutePath, byte[] content) throws Exception {
        delegate.uploadFile(absolutePath, content);
    }

    @Override
    public byte[] downloadFile(String absolutePath) throws Exception {
        return delegate.downloadFile(absolutePath);
    }

    public String leaseId() {
        return lease.leaseId();
    }

    public String workspaceId() {
        return lease.workspaceId();
    }

    RedisOpenSandboxClient owner() {
        return owner;
    }

    OpenSandbox delegate() {
        return delegate;
    }

    OpenSandboxActiveLease lease() {
        return lease;
    }
}
