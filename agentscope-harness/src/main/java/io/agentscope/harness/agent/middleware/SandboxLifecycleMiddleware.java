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
package io.agentscope.harness.agent.middleware;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.filesystem.sandbox.SandboxBackedFilesystem;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxAcquireResult;
import io.agentscope.harness.agent.sandbox.SandboxContext;
import io.agentscope.harness.agent.sandbox.SandboxManager;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Middleware that manages the sandbox session lifecycle around each agent call.
 *
 * <h2>Pre-{@code next.apply}</h2>
 * <ol>
 *   <li>Read {@link SandboxContext} from the current {@link RuntimeContext}</li>
 *   <li>Acquire a session via {@link SandboxManager}</li>
 *   <li>Start the session (4-branch workspace init)</li>
 *   <li>Bind the live session on the per-call {@link RuntimeContext} for the
 *       {@link SandboxBackedFilesystem} proxy to resolve</li>
 * </ol>
 *
 * <h2>doFinally</h2>
 * <ol>
 *   <li>Release the session via {@link SandboxManager} (stop + optional shutdown)</li>
 *   <li>Persist sandbox session state via {@link SandboxManager} and
 *       {@link io.agentscope.harness.agent.sandbox.SessionSandboxStateStore}</li>
 *   <li>Invalidate this call's sandbox acquisition across {@link RuntimeContext} copies</li>
 * </ol>
 *
 * <p>Post-call failures (persist, release) are logged but do not propagate — this ensures
 * the agent call result is always returned to the caller even if sandbox cleanup fails.
 *
 * <p>The sandbox is bound <em>per call</em> on the invocation's {@link RuntimeContext} rather than
 * on a shared agent-level slot: distinct {@code (userId, sessionId)} sessions run in parallel on
 * the same agent bean, so a shared slot would let concurrent calls corrupt each other's binding
 * (issue #2490).
 */
public class SandboxLifecycleMiddleware implements HarnessRuntimeMiddleware {

    private static final Logger log = LoggerFactory.getLogger(SandboxLifecycleMiddleware.class);

    private final SandboxManager sandboxManager;
    private final SandboxBackedFilesystem filesystemProxy;
    private volatile Consumer<RuntimeContext> beforeStartCallback;

    public SandboxLifecycleMiddleware(
            SandboxManager sandboxManager, SandboxBackedFilesystem filesystemProxy) {
        this.sandboxManager = sandboxManager;
        this.filesystemProxy = filesystemProxy;
    }

    /**
     * Session lifecycle is driven explicitly by HarnessAgent; participates at no point.
     * Subclasses overriding hooks must re-declare.
     */
    @Override
    public Set<ExtensionPoint> activePoints() {
        return EnumSet.noneOf(ExtensionPoint.class);
    }

    /**
     * Registers a callback that runs after the sandbox session is acquired but before
     * {@link io.agentscope.harness.agent.sandbox.Sandbox#start()} applies workspace projection.
     * This allows callers to materialise resources on the host workspace (e.g.
     * {@code .skills-cache/}) so that projection picks them up in the same call.
     *
     * @param callback receives the per-call {@link RuntimeContext}; may be {@code null} to clear
     */
    public void setBeforeStartCallback(Consumer<RuntimeContext> callback) {
        this.beforeStartCallback = callback;
    }

    /**
     * Acquires the sandbox for the current call, binding it to the call's runtime context.
     *
     * <p>Intended to be wrapped by the caller in a {@code Mono.using}/{@code Flux.using} so that
     * {@link #releaseForCall} is always invoked on termination (complete/error/cancel). This
     * method does <b>not</b> serialize same-session concurrency; callers must ensure per-session
     * non-concurrency at the entry point, or configure a distributed {@code SandboxExecutionGuard}
     * whose lease serializes by {@code SandboxIsolationKey}.
     */
    public void acquireForCall(RuntimeContext ctx) {
        if (ctx == null) {
            return;
        }
        SandboxContext sandboxContext = ctx.get(SandboxContext.class);
        if (sandboxContext == null) {
            return;
        }
        doAcquire(ctx, sandboxContext);
    }

    /**
     * Releases the sandbox acquired by {@link #acquireForCall}. Intended as the cleanup step of
     * a {@code Mono.using}/{@code Flux.using} wrapper; failures are logged but do not propagate.
     */
    public void releaseForCall(RuntimeContext ctx) {
        doRelease(ctx);
    }

    /**
     * Acquires a sandbox session: runs the optional {@link #beforeStartCallback},
     * obtains a lease via {@link SandboxManager#acquire}, starts the sandbox, and
     * binds it to the filesystem proxy for this call.
     *
     * <p>On failure after acquire, the sandbox is released and the lease closed to
     * prevent resource leaks.
     */
    private void doAcquire(RuntimeContext ctx, SandboxContext sandboxContext) {
        try {
            Consumer<RuntimeContext> cb = beforeStartCallback;
            if (cb != null) {
                try {
                    cb.accept(ctx);
                } catch (Exception e) {
                    log.warn(
                            "[sandbox-mw] beforeStartCallback failed; proceeding with sandbox"
                                    + " start: {}",
                            e.getMessage(),
                            e);
                }
            }
            SandboxAcquireResult result = sandboxManager.acquire(sandboxContext, ctx);
            Sandbox sandbox = result.getSandbox();
            try {
                sandbox.start();
                // RuntimeContext is the concurrency-safe binding for this exact invocation.
                ctx.put(SandboxAcquireResult.class, result);
                filesystemProxy.setSandbox(sandbox);
                log.debug(
                        "[sandbox-mw] Acquired sandbox {}",
                        sandbox.getState() != null ? sandbox.getState().getSessionId() : "?");
            } catch (Exception e) {
                ctx.put(SandboxAcquireResult.class, null);
                filesystemProxy.clearSandboxIfCurrent(sandbox);
                try {
                    sandboxManager.release(result);
                } catch (Exception releaseErr) {
                    log.warn(
                            "[sandbox-mw] Failed to release session after pre-call failure: {}",
                            releaseErr.getMessage(),
                            releaseErr);
                }
                result.getLease().close();
                throw e;
            }
        } catch (Exception e) {
            log.error("[sandbox-mw] Failed to acquire/start sandbox", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Releases a previously acquired sandbox session: persists state, releases the
     * sandbox via {@link SandboxManager#release}, and closes the lease.
     *
     * <p>Failures during persist or release are logged but do not propagate, ensuring
     * the agent call result is always delivered to the caller.
     */
    private void doRelease(RuntimeContext ctx) {
        if (ctx == null) {
            return;
        }
        // The binding is established by this exact invocation and carried by RuntimeContext.
        SandboxAcquireResult result = ctx.get(SandboxAcquireResult.class);
        if (result == null) {
            return;
        }
        // Retain an invalidated binding so this context cannot fall back to a sibling call.
        if (!result.beginRelease()) {
            return;
        }
        // Compare-and-clear the fallback field so a releasing call never nulls a concurrent
        // sibling's binding (issue #2490); it only clears the field when it still points here.
        filesystemProxy.clearSandboxIfCurrent(result.getSandbox());
        SandboxContext sandboxContext = ctx.get(SandboxContext.class);
        // Release (stop/persist workspace) first so state mutations made during stop — e.g.
        // workspaceRootReady or per-session snapshot records — are captured by the persist below.
        try {
            sandboxManager.release(result);
        } catch (Exception e) {
            log.warn("[sandbox-mw] Failed to release sandbox session: {}", e.getMessage(), e);
        }
        try {
            sandboxManager.persistState(result, sandboxContext, ctx);
        } catch (Exception e) {
            log.warn("[sandbox-mw] Failed to persist sandbox state: {}", e.getMessage(), e);
        }
        result.getLease().close();
    }
}
