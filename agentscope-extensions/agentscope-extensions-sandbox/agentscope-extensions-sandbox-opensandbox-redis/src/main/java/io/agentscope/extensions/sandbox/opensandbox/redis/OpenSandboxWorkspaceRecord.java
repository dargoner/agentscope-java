/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import java.time.Instant;

/** JSON-serializable source of truth for one managed OpenSandbox workspace. */
public final class OpenSandboxWorkspaceRecord {
    static final int CURRENT_SCHEMA_VERSION = 2;

    public enum LifecycleState {
        CREATING,
        RUNNING,
        EVICTION_PENDING,
        PAUSED,
        ABSENT,
        ERROR
    }

    private int schemaVersion = CURRENT_SCHEMA_VERSION;
    private String workspaceId;
    private String isolationScope;
    private String agentId;
    private String sandboxId;
    private String runtimeImage;
    private String runtimeProfileHash;
    private LifecycleState lifecycleState = LifecycleState.ABSENT;
    private long generation;
    private String nativeSnapshotId;
    private String previousNativeSnapshotId;
    private Instant lastAccessAt;
    private Instant pausedAt;
    private Instant expiresAt;
    private Instant evictionCandidateAt;
    private boolean dirty;
    private String lastError;
    private Instant updatedAt;

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getIsolationScope() {
        return isolationScope;
    }

    public void setIsolationScope(String isolationScope) {
        this.isolationScope = isolationScope;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public String getSandboxId() {
        return sandboxId;
    }

    public void setSandboxId(String sandboxId) {
        this.sandboxId = sandboxId;
    }

    public String getRuntimeImage() {
        return runtimeImage;
    }

    public void setRuntimeImage(String runtimeImage) {
        this.runtimeImage = runtimeImage;
    }

    public String getRuntimeProfileHash() {
        return runtimeProfileHash;
    }

    public void setRuntimeProfileHash(String runtimeProfileHash) {
        this.runtimeProfileHash = runtimeProfileHash;
    }

    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public void setLifecycleState(LifecycleState lifecycleState) {
        this.lifecycleState = lifecycleState;
    }

    public long getGeneration() {
        return generation;
    }

    public void setGeneration(long generation) {
        this.generation = generation;
    }

    public String getNativeSnapshotId() {
        return nativeSnapshotId;
    }

    public void setNativeSnapshotId(String nativeSnapshotId) {
        this.nativeSnapshotId = nativeSnapshotId;
    }

    public String getPreviousNativeSnapshotId() {
        return previousNativeSnapshotId;
    }

    public void setPreviousNativeSnapshotId(String previousNativeSnapshotId) {
        this.previousNativeSnapshotId = previousNativeSnapshotId;
    }

    public Instant getLastAccessAt() {
        return lastAccessAt;
    }

    public void setLastAccessAt(Instant lastAccessAt) {
        this.lastAccessAt = lastAccessAt;
    }

    public Instant getPausedAt() {
        return pausedAt;
    }

    public void setPausedAt(Instant pausedAt) {
        this.pausedAt = pausedAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }

    public Instant getEvictionCandidateAt() {
        return evictionCandidateAt;
    }

    public void setEvictionCandidateAt(Instant evictionCandidateAt) {
        this.evictionCandidateAt = evictionCandidateAt;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public OpenSandboxWorkspaceRecord copy() {
        OpenSandboxWorkspaceRecord copy = new OpenSandboxWorkspaceRecord();
        copy.schemaVersion = schemaVersion;
        copy.workspaceId = workspaceId;
        copy.isolationScope = isolationScope;
        copy.agentId = agentId;
        copy.sandboxId = sandboxId;
        copy.runtimeImage = runtimeImage;
        copy.runtimeProfileHash = runtimeProfileHash;
        copy.lifecycleState = lifecycleState;
        copy.generation = generation;
        copy.nativeSnapshotId = nativeSnapshotId;
        copy.previousNativeSnapshotId = previousNativeSnapshotId;
        copy.lastAccessAt = lastAccessAt;
        copy.pausedAt = pausedAt;
        copy.expiresAt = expiresAt;
        copy.evictionCandidateAt = evictionCandidateAt;
        copy.dirty = dirty;
        copy.lastError = lastError;
        copy.updatedAt = updatedAt;
        return copy;
    }
}
