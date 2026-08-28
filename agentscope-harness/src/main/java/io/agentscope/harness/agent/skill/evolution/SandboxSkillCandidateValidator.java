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

import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.evolution.SkillCandidateArtifact;
import io.agentscope.core.skill.evolution.SkillCandidateValidator;
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** Executes candidate validation inside an existing AgentScope Sandbox. */
public final class SandboxSkillCandidateValidator implements SkillCandidateValidator {

    private static final int DEFAULT_MAX_FEEDBACK_CHARACTERS = 8_192;

    private final Supplier<? extends Sandbox> sandboxFactory;
    private final boolean ownsSandbox;
    private final Scheduler blockingScheduler;
    private final int maxFeedbackCharacters;

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
                            Mono.fromSupplier(sandboxFactory),
                            sandbox -> execute(sandbox, candidate, request),
                            this::cleanup,
                            (sandbox, ignored) -> cleanup(sandbox),
                            this::cleanup);
                });
    }

    private Mono<SkillValidationReport> execute(
            Sandbox sandbox, SkillCandidateArtifact candidate, SkillValidationRequest request) {
        return Mono.fromCallable(
                        () -> {
                            if (!sandbox.isRunning()) {
                                sandbox.start();
                            }
                            int timeoutSeconds = timeoutSeconds(request.timeout());
                            ExecResult materialized =
                                    sandbox.exec(
                                            null,
                                            materializeCommand(
                                                    sandbox.getWorkspaceRoot(),
                                                    candidate.candidate()),
                                            timeoutSeconds);
                            if (!materialized.ok() || materialized.truncated()) {
                                return report(
                                        candidate,
                                        request.stage(),
                                        materialized,
                                        false,
                                        "候选技能写入隔离工作区失败");
                            }
                            String candidateRoot =
                                    normalizeRoot(sandbox.getWorkspaceRoot()) + "/skill-candidate";
                            String command =
                                    "export SKILL_CANDIDATE_ROOT="
                                            + shellQuote(candidateRoot)
                                            + "; cd "
                                            + shellQuote(candidateRoot)
                                            + "; "
                                            + requiredCommand(request.validationSpec());
                            ExecResult result = executeValidation(sandbox, command, timeoutSeconds);
                            int expectedExitCode = expectedExitCode(request.validationSpec());
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

    private static ExecResult executeValidation(Sandbox sandbox, String command, int timeoutSeconds)
            throws Exception {
        try {
            return sandbox.exec(null, command, timeoutSeconds);
        } catch (SandboxException.ExecException exception) {
            return new ExecResult(
                    exception.getExitCode(), exception.getStdout(), exception.getStderr(), false);
        }
    }

    private Mono<Void> cleanup(Sandbox sandbox) {
        return Mono.fromCallable(
                        () -> {
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
                            return true;
                        })
                .subscribeOn(blockingScheduler)
                .then();
    }

    private SkillValidationReport report(
            SkillCandidateArtifact candidate,
            SkillValidationStage stage,
            ExecResult result,
            boolean passed,
            String feedback) {
        Map<String, Number> metrics =
                Map.of(
                        "exitCode",
                        result.exitCode(),
                        "outputTruncated",
                        result.truncated() ? 1 : 0);
        Map<String, Object> disclosed =
                Map.of(
                        "schemaVersion", 1,
                        "summary", bounded(feedback),
                        "exitCode", result.exitCode(),
                        "outputTruncated", result.truncated());
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

    private String materializeCommand(String workspaceRoot, AgentSkill skill) {
        String targetRoot = normalizeRoot(workspaceRoot) + "/skill-candidate";
        StringBuilder command =
                new StringBuilder("set -eu; mkdir -p ").append(shellQuote(targetRoot));
        appendFile(command, targetRoot + "/SKILL.md", skill.getSkillContent());
        skill.getResources().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(
                        entry -> {
                            String relativePath = normalizeRelativePath(entry.getKey());
                            String target = targetRoot + "/" + relativePath;
                            int separator = target.lastIndexOf('/');
                            command.append("; mkdir -p ")
                                    .append(shellQuote(target.substring(0, separator)));
                            appendFile(command, target, entry.getValue());
                        });
        return command.toString();
    }

    private static void appendFile(StringBuilder command, String target, String content) {
        String encoded =
                Base64.getEncoder()
                        .encodeToString(
                                (content == null ? "" : content).getBytes(StandardCharsets.UTF_8));
        command.append("; printf %s ")
                .append(shellQuote(encoded))
                .append(" | base64 -d > ")
                .append(shellQuote(target));
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

    private static String normalizeRelativePath(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("resource path must not be blank");
        }
        String path = value.replace('\\', '/');
        while (path.startsWith("./")) {
            path = path.substring(2);
        }
        if (path.startsWith("/")
                || path.equals("..")
                || path.startsWith("../")
                || path.contains("/../")) {
            throw new IllegalArgumentException("resource path must remain inside the candidate");
        }
        return path;
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
}
