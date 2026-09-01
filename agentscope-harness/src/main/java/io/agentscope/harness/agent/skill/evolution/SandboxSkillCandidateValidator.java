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
package io.agentscope.harness.agent.skill.evolution;

import io.agentscope.core.skill.evolution.SkillArtifactMaterializer;
import io.agentscope.core.skill.evolution.SkillCandidateArtifact;
import io.agentscope.core.skill.evolution.SkillCandidateValidator;
import io.agentscope.core.skill.evolution.SkillEvolutionPayload;
import io.agentscope.core.skill.evolution.SkillValidationReport;
import io.agentscope.core.skill.evolution.SkillValidationRequest;
import io.agentscope.core.skill.evolution.SkillValidationStage;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxClient;
import io.agentscope.harness.agent.sandbox.SandboxClientOptions;
import io.agentscope.harness.agent.sandbox.SandboxException;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.snapshot.SandboxSnapshotSpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** Executes candidate validation inside an existing AgentScope Sandbox. */
public final class SandboxSkillCandidateValidator implements SkillCandidateValidator {

    private static final int DEFAULT_MAX_FEEDBACK_CHARACTERS = 8_192;
    private static final int CLEANUP_TIMEOUT_SECONDS = 10;
    private static final String CANDIDATE_ROOT_PREFIX = ".agentscope-skill-validation-";

    private final Supplier<? extends Sandbox> sandboxFactory;
    private final boolean ownsSandbox;
    private final Scheduler blockingScheduler;
    private final int maxFeedbackCharacters;
    private final Map<Sandbox, Integer> activeInvocations = new IdentityHashMap<>();

    /** Uses an externally owned sandbox and never shuts it down. */
    public SandboxSkillCandidateValidator(Sandbox sandbox) {
        this(() -> sandbox, false, Schedulers.boundedElastic(), DEFAULT_MAX_FEEDBACK_CHARACTERS);
    }

    /** Creates an adapter whose sandbox is owned by this validator invocation. */
    public static <O extends SandboxClientOptions> SandboxSkillCandidateValidator owned(
            SandboxClient<O> client,
            WorkspaceSpec workspaceSpec,
            SandboxSnapshotSpec snapshotSpec,
            O options) {
        Objects.requireNonNull(client, "client must not be null");
        return new SandboxSkillCandidateValidator(
                () -> client.create(workspaceSpec, snapshotSpec, options),
                true,
                Schedulers.boundedElastic(),
                DEFAULT_MAX_FEEDBACK_CHARACTERS);
    }

    /** Creates a validator with explicit ownership and scheduler for integration testing. */
    public SandboxSkillCandidateValidator(
            Supplier<? extends Sandbox> sandboxFactory,
            boolean ownsSandbox,
            Scheduler blockingScheduler,
            int maxFeedbackCharacters) {
        this.sandboxFactory =
                Objects.requireNonNull(sandboxFactory, "sandboxFactory must not be null");
        this.ownsSandbox = ownsSandbox;
        this.blockingScheduler =
                Objects.requireNonNull(blockingScheduler, "blockingScheduler must not be null");
        if (maxFeedbackCharacters <= 0) {
            throw new IllegalArgumentException("maxFeedbackCharacters must be positive");
        }
        this.maxFeedbackCharacters = maxFeedbackCharacters;
    }

    @Override
    public Mono<SkillValidationReport> validate(
            SkillCandidateArtifact candidate, SkillValidationRequest request) {
        return Mono.defer(
                () -> {
                    Objects.requireNonNull(candidate, "candidate must not be null");
                    Objects.requireNonNull(request, "request must not be null");
                    if (!candidate.verifyIntegrity()) {
                        return Mono.error(
                                new IllegalArgumentException("candidate integrity check failed"));
                    }
                    return Mono.usingWhen(
                            Mono.fromSupplier(this::acquireInvocation),
                            invocation -> execute(invocation, candidate, request),
                            this::cleanup,
                            (invocation, ignored) -> cleanup(invocation),
                            this::cleanup);
                });
    }

    private Mono<SkillValidationReport> execute(
            ValidationInvocation invocation,
            SkillCandidateArtifact candidate,
            SkillValidationRequest request) {
        return Mono.fromCallable(
                        () -> {
                            Sandbox sandbox = invocation.sandbox();
                            ensureRunning(invocation);
                            int timeoutSeconds = timeoutSeconds(request.timeout());
                            invocation.markCandidateRootCreationAttempted();
                            ExecResult materialized =
                                    sandbox.exec(
                                            null,
                                            materializeCommand(
                                                    invocation.candidateRoot(),
                                                    SkillArtifactMaterializer.materialize(
                                                            candidate.candidate())),
                                            timeoutSeconds);
                            if (!materialized.ok() || materialized.truncated()) {
                                return report(
                                        candidate,
                                        request.stage(),
                                        materialized,
                                        false,
                                        "候选技能写入隔离工作区失败");
                            }
                            String command =
                                    "set -eu; candidate_root="
                                            + shellQuote(invocation.candidateRoot())
                                            + "; [ -d \"$candidate_root\" ]"
                                            + "; [ ! -L \"$candidate_root\" ]"
                                            + "; export SKILL_CANDIDATE_ROOT=\"$candidate_root\""
                                            + "; cd \"$candidate_root\""
                                            + "; "
                                            + requiredCommand(request.validationSpec().data());
                            ExecResult result = executeValidation(sandbox, command, timeoutSeconds);
                            int expectedExitCode =
                                    expectedExitCode(request.validationSpec().data());
                            boolean passed =
                                    result.exitCode() == expectedExitCode && !result.truncated();
                            return report(
                                    candidate,
                                    request.stage(),
                                    result,
                                    passed,
                                    bounded(result.combinedOutput()));
                        })
                .subscribeOn(blockingScheduler)
                .timeout(request.timeout());
    }

    private ValidationInvocation acquireInvocation() {
        Sandbox sandbox =
                Objects.requireNonNull(sandboxFactory.get(), "sandboxFactory returned null");
        synchronized (activeInvocations) {
            activeInvocations.merge(sandbox, 1, Integer::sum);
        }
        return new ValidationInvocation(sandbox, CANDIDATE_ROOT_PREFIX + UUID.randomUUID());
    }

    private static void ensureRunning(ValidationInvocation invocation) throws Exception {
        synchronized (invocation.sandbox()) {
            if (!invocation.sandbox().isRunning()) {
                invocation.sandbox().start();
            }
            invocation.initializeCandidateRoot(invocation.sandbox().getWorkspaceRoot());
        }
    }

    private static ExecResult executeValidation(Sandbox sandbox, String command, int timeoutSeconds)
            throws Exception {
        try {
            return sandbox.exec(null, command, timeoutSeconds);
        } catch (SandboxException.ExecException exception) {
            return new ExecResult(
                    exception.getExitCode(), exception.getStdout(), exception.getStderr(), false);
        }
    }

    private Mono<Void> cleanup(ValidationInvocation invocation) {
        return Mono.fromCallable(
                        () -> {
                            bestEffortDeleteCandidateRoot(invocation);
                            cleanupSandboxIfLast(invocation.sandbox());
                            return true;
                        })
                .subscribeOn(blockingScheduler)
                .then();
    }

    private static void bestEffortDeleteCandidateRoot(ValidationInvocation invocation) {
        if (!invocation.candidateRootCreationAttempted()) {
            return;
        }
        try {
            if (!invocation.sandbox().isRunning()) {
                return;
            }
            String command =
                    "set -eu; candidate_root="
                            + shellQuote(invocation.candidateRoot())
                            + "; if [ -L \"$candidate_root\" ]; then"
                            + " rm -f -- \"$candidate_root\""
                            + "; elif [ -e \"$candidate_root\" ]; then"
                            + " rm -rf -- \"$candidate_root\""
                            + "; fi";
            invocation.sandbox().exec(null, command, CLEANUP_TIMEOUT_SECONDS);
        } catch (Exception ignored) {
            // Candidate directories are best-effort cleanup. Sandbox lifecycle cleanup still runs.
        }
    }

    private void cleanupSandboxIfLast(Sandbox sandbox) throws Exception {
        synchronized (activeInvocations) {
            Integer count = activeInvocations.get(sandbox);
            if (count != null && count > 1) {
                activeInvocations.put(sandbox, count - 1);
                return;
            }
            activeInvocations.remove(sandbox);

            Exception failure = null;
            try {
                sandbox.stop();
            } catch (Exception exception) {
                failure = exception;
            }
            if (ownsSandbox) {
                try {
                    sandbox.shutdown();
                } catch (Exception exception) {
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private SkillValidationReport report(
            SkillCandidateArtifact candidate,
            SkillValidationStage stage,
            ExecResult result,
            boolean passed,
            String feedback) {
        SkillEvolutionPayload metrics =
                SkillEvolutionPayload.versionOne(
                        "agentscope.skill-evolution.validation-metrics",
                        Map.of(
                                "exitCode",
                                result.exitCode(),
                                "outputTruncated",
                                result.truncated() ? 1 : 0));
        SkillEvolutionPayload disclosed =
                SkillEvolutionPayload.versionOne(
                        "agentscope.skill-evolution.validation-feedback",
                        Map.of(
                                "summary", bounded(feedback),
                                "exitCode", result.exitCode(),
                                "outputTruncated", result.truncated()));
        String reportHash =
                sha256(
                        candidate.contentHash()
                                + "\n"
                                + stage.name()
                                + "\n"
                                + passed
                                + "\n"
                                + result.exitCode()
                                + "\n"
                                + bounded(result.combinedOutput()));
        return new SkillValidationReport(
                stage,
                passed,
                reportHash,
                metrics,
                stage == SkillValidationStage.DEVELOPMENT
                        ? java.util.Optional.of(disclosed)
                        : java.util.Optional.empty());
    }

    private String materializeCommand(String targetRoot, Map<String, byte[]> files) {
        StringBuilder command =
                new StringBuilder("set -eu; umask 077; candidate_root=")
                        .append(shellQuote(targetRoot))
                        .append(
                                "; if [ -e \"$candidate_root\" ]"
                                        + " || [ -L \"$candidate_root\" ]; then")
                        .append(" echo 'candidate validation root already exists' >&2; exit 73; fi")
                        .append("; mkdir -- \"$candidate_root\"")
                        .append("; trap 'rm -rf -- \"$candidate_root\"' 0 1 2 15")
                        .append("; [ -d \"$candidate_root\" ]")
                        .append("; [ ! -L \"$candidate_root\" ]");
        files.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(
                        entry -> {
                            String target = targetRoot + "/" + entry.getKey();
                            appendSafeDirectories(command, targetRoot, entry.getKey());
                            appendFile(command, target, entry.getValue());
                        });
        command.append("; trap - 0 1 2 15");
        return command.toString();
    }

    private static void appendSafeDirectories(
            StringBuilder command, String targetRoot, String relativePath) {
        String[] parts = relativePath.split("/");
        String current = targetRoot;
        for (int index = 0; index < parts.length - 1; index++) {
            current += "/" + parts[index];
            command.append("; if [ ! -e ")
                    .append(shellQuote(current))
                    .append(" ] && [ ! -L ")
                    .append(shellQuote(current))
                    .append(" ]; then mkdir -- ")
                    .append(shellQuote(current))
                    .append("; fi; [ -d ")
                    .append(shellQuote(current))
                    .append(" ]; [ ! -L ")
                    .append(shellQuote(current))
                    .append(" ]");
        }
    }

    private static void appendFile(StringBuilder command, String target, byte[] content) {
        String encoded = Base64.getEncoder().encodeToString(content);
        command.append("; [ ! -e ")
                .append(shellQuote(target))
                .append(" ]; [ ! -L ")
                .append(shellQuote(target))
                .append(" ]; printf %s ")
                .append(shellQuote(encoded))
                .append(" | base64 -d > ")
                .append(shellQuote(target))
                .append("; [ -f ")
                .append(shellQuote(target))
                .append(" ]; [ ! -L ")
                .append(shellQuote(target))
                .append(" ]");
    }

    private static String requiredCommand(Map<String, Object> validationSpec) {
        Object command = validationSpec.get("command");
        if (!(command instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException("validationSpec.command must not be blank");
        }
        return text.trim();
    }

    private static int expectedExitCode(Map<String, Object> validationSpec) {
        Object value = validationSpec.getOrDefault("expectedExitCode", 0);
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("validationSpec.expectedExitCode must be a number");
        }
        return number.intValue();
    }

    private String bounded(String value) {
        String text = value == null ? "" : value;
        return text.length() <= maxFeedbackCharacters
                ? text
                : text.substring(0, maxFeedbackCharacters) + "…";
    }

    private static int timeoutSeconds(Duration timeout) {
        long seconds = Math.max(1, timeout.plusMillis(999).toSeconds());
        return (int) Math.min(Integer.MAX_VALUE, seconds);
    }

    private static String normalizeRoot(String value) {
        String root = value == null || value.isBlank() ? "/workspace" : value.trim();
        return root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
    }

    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    private static String sha256(String value) {
        try {
            byte[] hash =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(hash.length * 2);
            for (byte item : hash) {
                result.append(String.format(Locale.ROOT, "%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static final class ValidationInvocation {

        private final Sandbox sandbox;
        private final String candidateDirectoryName;
        private final AtomicBoolean candidateRootCreationAttempted = new AtomicBoolean();
        private volatile String candidateRoot;

        private ValidationInvocation(Sandbox sandbox, String candidateDirectoryName) {
            this.sandbox = sandbox;
            this.candidateDirectoryName = candidateDirectoryName;
        }

        private Sandbox sandbox() {
            return sandbox;
        }

        private String candidateRoot() {
            String resolved = candidateRoot;
            if (resolved == null) {
                throw new IllegalStateException("candidate root has not been initialized");
            }
            return resolved;
        }

        private void initializeCandidateRoot(String workspaceRoot) {
            if (candidateRoot == null) {
                candidateRoot = normalizeRoot(workspaceRoot) + "/" + candidateDirectoryName;
            }
        }

        private void markCandidateRootCreationAttempted() {
            candidateRootCreationAttempted.set(true);
        }

        private boolean candidateRootCreationAttempted() {
            return candidateRootCreationAttempted.get();
        }
    }
}
