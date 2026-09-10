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

import java.time.Duration;
import java.util.Objects;

/** Timing and coordination settings for clustered OpenSandbox workspace lifecycle. */
public final class OpenSandboxRedisLifecycleOptions {
    private Duration idleTtl = Duration.ofHours(1);
    private Duration sweepInterval = Duration.ofMinutes(5);
    private Duration evictionGrace = Duration.ofMinutes(5);
    private Duration heartbeatInterval = Duration.ofSeconds(30);
    private Duration activeLeaseTtl = Duration.ofSeconds(180);
    private Duration activeSandboxTtl = Duration.ofMinutes(10);
    private Duration activeRenewLead = Duration.ofMinutes(6);
    private Duration pauseRetention = Duration.ofMinutes(5);
    private Duration snapshotReadyTimeout = Duration.ofMinutes(5);
    private Duration lockWait = Duration.ofSeconds(10);
    private Duration orphanGrace = Duration.ofMinutes(10);
    private boolean sweeperEnabled = true;

    public Duration getIdleTtl() {
        return idleTtl;
    }

    public void setIdleTtl(Duration idleTtl) {
        Duration previous = this.idleTtl;
        this.idleTtl = positive(idleTtl, "idleTtl");
        validateOrRestore(() -> this.idleTtl = previous);
    }

    public Duration getSweepInterval() {
        return sweepInterval;
    }

    public void setSweepInterval(Duration sweepInterval) {
        Duration previous = this.sweepInterval;
        this.sweepInterval = positive(sweepInterval, "sweepInterval");
        validateOrRestore(() -> this.sweepInterval = previous);
    }

    public Duration getEvictionGrace() {
        return evictionGrace;
    }

    public void setEvictionGrace(Duration evictionGrace) {
        Duration previous = this.evictionGrace;
        this.evictionGrace = positive(evictionGrace, "evictionGrace");
        validateOrRestore(() -> this.evictionGrace = previous);
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        Duration previous = this.heartbeatInterval;
        this.heartbeatInterval = positive(heartbeatInterval, "heartbeatInterval");
        validateOrRestore(() -> this.heartbeatInterval = previous);
    }

    public Duration getActiveLeaseTtl() {
        return activeLeaseTtl;
    }

    public void setActiveLeaseTtl(Duration activeLeaseTtl) {
        Duration previous = this.activeLeaseTtl;
        this.activeLeaseTtl = positive(activeLeaseTtl, "activeLeaseTtl");
        validateOrRestore(() -> this.activeLeaseTtl = previous);
    }

    public Duration getActiveSandboxTtl() {
        return activeSandboxTtl;
    }

    public void setActiveSandboxTtl(Duration activeSandboxTtl) {
        Duration previous = this.activeSandboxTtl;
        this.activeSandboxTtl = positive(activeSandboxTtl, "activeSandboxTtl");
        validateOrRestore(() -> this.activeSandboxTtl = previous);
    }

    public Duration getActiveRenewLead() {
        return activeRenewLead;
    }

    public void setActiveRenewLead(Duration activeRenewLead) {
        Duration previous = this.activeRenewLead;
        this.activeRenewLead = positive(activeRenewLead, "activeRenewLead");
        validateOrRestore(() -> this.activeRenewLead = previous);
    }

    public Duration getPauseRetention() {
        return pauseRetention;
    }

    public void setPauseRetention(Duration pauseRetention) {
        this.pauseRetention = positive(pauseRetention, "pauseRetention");
    }

    public Duration getSnapshotReadyTimeout() {
        return snapshotReadyTimeout;
    }

    public void setSnapshotReadyTimeout(Duration snapshotReadyTimeout) {
        Duration previous = this.snapshotReadyTimeout;
        this.snapshotReadyTimeout = positive(snapshotReadyTimeout, "snapshotReadyTimeout");
        validateOrRestore(() -> this.snapshotReadyTimeout = previous);
    }

    public Duration getLockWait() {
        return lockWait;
    }

    public void setLockWait(Duration lockWait) {
        this.lockWait = positive(lockWait, "lockWait");
    }

    public Duration getOrphanGrace() {
        return orphanGrace;
    }

    public void setOrphanGrace(Duration orphanGrace) {
        this.orphanGrace = positive(orphanGrace, "orphanGrace");
    }

    public boolean isSweeperEnabled() {
        return sweeperEnabled;
    }

    public void setSweeperEnabled(boolean sweeperEnabled) {
        this.sweeperEnabled = sweeperEnabled;
    }

    public static OpenSandboxRedisLifecycleOptions copyOf(OpenSandboxRedisLifecycleOptions source) {
        Objects.requireNonNull(source, "source");
        OpenSandboxRedisLifecycleOptions copy = new OpenSandboxRedisLifecycleOptions();
        copy.idleTtl = source.idleTtl;
        copy.sweepInterval = source.sweepInterval;
        copy.evictionGrace = source.evictionGrace;
        copy.heartbeatInterval = source.heartbeatInterval;
        copy.activeLeaseTtl = source.activeLeaseTtl;
        copy.activeSandboxTtl = source.activeSandboxTtl;
        copy.activeRenewLead = source.activeRenewLead;
        copy.pauseRetention = source.pauseRetention;
        copy.snapshotReadyTimeout = source.snapshotReadyTimeout;
        copy.lockWait = source.lockWait;
        copy.orphanGrace = source.orphanGrace;
        copy.sweeperEnabled = source.sweeperEnabled;
        copy.validate();
        return copy;
    }

    public void validate() {
        if (heartbeatInterval.multipliedBy(3).compareTo(activeLeaseTtl) > 0) {
            throw new IllegalArgumentException(
                    "activeLeaseTtl must be at least three heartbeat intervals");
        }
        if (sweepInterval.compareTo(idleTtl) >= 0) {
            throw new IllegalArgumentException("sweepInterval must be shorter than idleTtl");
        }
        if (activeRenewLead.compareTo(activeSandboxTtl) >= 0) {
            throw new IllegalArgumentException(
                    "activeRenewLead must be shorter than activeSandboxTtl");
        }
        if (snapshotReadyTimeout.compareTo(activeSandboxTtl) >= 0) {
            throw new IllegalArgumentException(
                    "snapshotReadyTimeout must be shorter than activeSandboxTtl");
        }
        if (evictionGrace.compareTo(heartbeatInterval) < 0) {
            throw new IllegalArgumentException(
                    "evictionGrace must be at least one heartbeat interval");
        }
    }

    private void validateOrRestore(Runnable restore) {
        try {
            validate();
        } catch (RuntimeException error) {
            restore.run();
            throw error;
        }
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
