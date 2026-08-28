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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.evolution.SkillArtifactMaterializer;
import io.agentscope.core.skill.evolution.SkillCandidateArtifact;
import io.agentscope.core.skill.evolution.SkillEvolutionPayload;
import io.agentscope.core.skill.evolution.SkillEvolutionType;
import io.agentscope.core.skill.evolution.SkillValidationReport;
import io.agentscope.core.skill.evolution.SkillValidationRequest;
import io.agentscope.core.skill.evolution.SkillValidationStage;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxState;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class SandboxSkillCandidateValidatorTest {

    @Test
    void validationIsColdAndDevelopmentFeedbackIsDisclosed() {
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, "passed", "", false));
        SandboxSkillCandidateValidator validator = new SandboxSkillCandidateValidator(sandbox);

        var result = validator.validate(candidate(), request(SkillValidationStage.DEVELOPMENT));

        assertEquals(0, sandbox.startCount);
        SkillValidationReport report = result.block();
        assertTrue(report.passed());
        assertTrue(report.disclosedFeedback().isPresent());
        assertEquals(1, sandbox.startCount);
        assertEquals(1, sandbox.stopCount);
        assertEquals(0, sandbox.shutdownCount);
        assertTrue(sandbox.commands.get(0).contains("base64 -d"));
        assertTrue(sandbox.commands.get(1).contains("SKILL_CANDIDATE_ROOT"));
        assertEquals(1, sandbox.cleanedRoots.size());
        assertTrue(sandbox.materializedRoots.isEmpty());
    }

    @Test
    void finalGateNeverDisclosesFeedbackAndOwnedSandboxIsDestroyed() {
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(2, "", "failed", false));
        SandboxSkillCandidateValidator validator =
                new SandboxSkillCandidateValidator(
                        () -> sandbox, true, Schedulers.boundedElastic(), 512);

        SkillValidationReport report =
                validator.validate(candidate(), request(SkillValidationStage.FINAL_GATE)).block();

        assertFalse(report.passed());
        assertTrue(report.disclosedFeedback().isEmpty());
        assertEquals(1, sandbox.stopCount);
        assertEquals(1, sandbox.shutdownCount);
    }

    @Test
    void validatorWritesTheExactCanonicalArtifactBytes() {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("name", "demo");
        metadata.put("description", "demo\r\nskill");
        metadata.put("settings", Map.of("z", 1.00d, "a", "first"));
        AgentSkill skill =
                new AgentSkill(
                        metadata,
                        "# Instructions\r\nValidate this body.\r\n",
                        Map.of("scripts\\verify.sh", "echo passed\r\n"),
                        null);
        SkillCandidateArtifact artifact =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), skill, 0);
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, "passed", "", false));

        new SandboxSkillCandidateValidator(sandbox)
                .validate(artifact, request(SkillValidationStage.DEVELOPMENT))
                .block();

        String command = sandbox.commands.get(0);
        String candidateRoot = candidateRoot(command);
        Map<String, byte[]> files = SkillArtifactMaterializer.materialize(artifact.candidate());
        files.forEach(
                (path, bytes) -> {
                    assertTrue(command.contains(candidateRoot + "/" + path));
                    assertTrue(command.contains(Base64.getEncoder().encodeToString(bytes)));
                });
        String skillMarkdown =
                new String(files.get(SkillArtifactMaterializer.SKILL_FILE), StandardCharsets.UTF_8);
        assertTrue(skillMarkdown.startsWith("---\n"));
        assertTrue(skillMarkdown.endsWith("# Instructions\nValidate this body.\n"));
        assertFalse(
                command.contains(
                        Base64.getEncoder()
                                .encodeToString(
                                        "# Instructions\nValidate this body.\n"
                                                .getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void sequentialValidationsUseFreshRootsAndRemoveAllCandidateFiles() {
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, "passed", "", false));
        SandboxSkillCandidateValidator validator = new SandboxSkillCandidateValidator(sandbox);

        validator
                .validate(
                        candidate("first", Map.of("resources/only-first.txt", "first")),
                        request(SkillValidationStage.DEVELOPMENT))
                .block();
        validator
                .validate(
                        candidate("second", Map.of("resources/only-second.txt", "second")),
                        request(SkillValidationStage.DEVELOPMENT))
                .block();

        List<String> materializeCommands = sandbox.materializeCommands();
        assertEquals(2, materializeCommands.size());
        String firstRoot = candidateRoot(materializeCommands.get(0));
        String secondRoot = candidateRoot(materializeCommands.get(1));
        assertNotEquals(firstRoot, secondRoot);
        assertTrue(firstRoot.contains("/.agentscope-skill-validation-"));
        assertTrue(secondRoot.contains("/.agentscope-skill-validation-"));
        assertTrue(materializeCommands.get(0).contains("only-first.txt"));
        assertFalse(materializeCommands.get(1).contains("only-first.txt"));
        assertTrue(materializeCommands.get(1).contains("only-second.txt"));
        assertTrue(materializeCommands.get(0).contains("[ -L \"$candidate_root\" ]"));
        assertTrue(materializeCommands.get(0).contains("already exists"));
        assertEquals(Set.of(firstRoot, secondRoot), Set.copyOf(sandbox.cleanedRoots));
        assertTrue(sandbox.materializedRoots.isEmpty());
        assertEquals(2, sandbox.stopCount);
    }

    @Test
    void concurrentValidationsAreIsolatedAndCleanupWaitsBeforeStoppingSharedSandbox() {
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, "passed", "", false));
        sandbox.requireConcurrentValidation = true;
        SandboxSkillCandidateValidator validator = new SandboxSkillCandidateValidator(sandbox);

        Mono.zip(
                        validator.validate(
                                candidate("first", Map.of("first.txt", "first")),
                                request(SkillValidationStage.DEVELOPMENT)),
                        validator.validate(
                                candidate("second", Map.of("second.txt", "second")),
                                request(SkillValidationStage.DEVELOPMENT)))
                .block(Duration.ofSeconds(5));

        List<String> validationCommands = sandbox.validationCommands();
        assertEquals(2, validationCommands.size());
        Set<String> validationRoots =
                Set.of(
                        candidateRoot(validationCommands.get(0)),
                        candidateRoot(validationCommands.get(1)));
        assertEquals(2, validationRoots.size());
        assertEquals(validationRoots, Set.copyOf(sandbox.cleanedRoots));
        assertTrue(sandbox.materializedRoots.isEmpty());
        assertFalse(sandbox.stoppedDuringValidation);
        assertEquals(1, sandbox.stopCount);
    }

    @Test
    void candidateDirectoryCleanupIsBestEffortAndOwnedSandboxStillShutsDown() {
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, "passed", "", false));
        sandbox.failCandidateRootCleanup = true;
        SandboxSkillCandidateValidator validator =
                new SandboxSkillCandidateValidator(
                        () -> sandbox, true, Schedulers.boundedElastic(), 512);

        SkillValidationReport report =
                validator.validate(candidate(), request(SkillValidationStage.FINAL_GATE)).block();

        assertTrue(report.passed());
        assertEquals(1, sandbox.cleanupAttempts.get());
        assertEquals(1, sandbox.stopCount);
        assertEquals(1, sandbox.shutdownCount);
    }

    @Test
    void materializationExceptionStillRemovesTheAttemptedCandidateRoot() {
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, "passed", "", false));
        sandbox.failMaterializationAfterCreatingRoot = true;
        SandboxSkillCandidateValidator validator = new SandboxSkillCandidateValidator(sandbox);

        assertThrows(
                RuntimeException.class,
                () ->
                        validator
                                .validate(candidate(), request(SkillValidationStage.DEVELOPMENT))
                                .block());

        assertEquals(1, sandbox.cleanupAttempts.get());
        assertEquals(1, sandbox.cleanedRoots.size());
        assertTrue(sandbox.materializedRoots.isEmpty());
        assertEquals(1, sandbox.stopCount);
    }

    @Test
    void timeoutStopsExecutionAndCleanupFailureIsPropagated() {
        FakeSandbox slow = new FakeSandbox(new ExecResult(0, "passed", "", false));
        slow.validationDelayMillis = 2_000;
        SandboxSkillCandidateValidator timeoutValidator =
                new SandboxSkillCandidateValidator(
                        () -> slow, false, Schedulers.boundedElastic(), 512);
        SkillValidationRequest timeoutRequest =
                new SkillValidationRequest(
                        SkillValidationStage.DEVELOPMENT,
                        payload("validation-spec", Map.of("command", "verify")),
                        payload("validation-constraints", Map.of()),
                        Duration.ofMillis(50));

        assertThrows(
                RuntimeException.class,
                () -> timeoutValidator.validate(candidate(), timeoutRequest).block());
        assertTrue(slow.stopCount > 0);

        FakeSandbox cleanupFailure = new FakeSandbox(new ExecResult(0, "passed", "", false));
        cleanupFailure.failStop = true;
        SandboxSkillCandidateValidator cleanupValidator =
                new SandboxSkillCandidateValidator(cleanupFailure);
        RuntimeException failure =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                cleanupValidator
                                        .validate(
                                                candidate(),
                                                request(SkillValidationStage.DEVELOPMENT))
                                        .block());
        assertTrue(rootMessage(failure).contains("cleanup failed"));
    }

    private static SkillCandidateArtifact candidate() {
        return candidate("demo", Map.of("scripts/verify.sh", "echo passed"));
    }

    private static SkillCandidateArtifact candidate(String name, Map<String, String> resources) {
        return SkillCandidateArtifact.create(
                SkillEvolutionType.CREATE,
                List.of(),
                new AgentSkill(name, name + " skill", "instructions", resources),
                0);
    }

    private static SkillValidationRequest request(SkillValidationStage stage) {
        return new SkillValidationRequest(
                stage,
                payload(
                        "validation-spec",
                        Map.of(
                                "command",
                                "test -f SKILL.md && echo passed",
                                "expectedExitCode",
                                0)),
                payload("validation-constraints", Map.of()),
                Duration.ofSeconds(2));
    }

    private static SkillEvolutionPayload payload(String schema, Map<String, Object> data) {
        return SkillEvolutionPayload.versionOne("test.skill-evolution." + schema, data);
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage();
    }

    private static String candidateRoot(String command) {
        Matcher matcher = Pattern.compile("candidate_root='([^']+)'").matcher(command);
        assertTrue(matcher.find(), () -> "candidate root missing from command: " + command);
        return matcher.group(1);
    }

    private static final class FakeSandbox implements Sandbox {

        private final ExecResult validationResult;
        private final List<String> commands = new CopyOnWriteArrayList<>();
        private final Set<String> materializedRoots = ConcurrentHashMap.newKeySet();
        private final List<String> cleanedRoots = new CopyOnWriteArrayList<>();
        private final AtomicInteger validationSequence = new AtomicInteger();
        private final AtomicInteger validationInFlight = new AtomicInteger();
        private final AtomicInteger cleanupAttempts = new AtomicInteger();
        private final CountDownLatch concurrentValidationEntered = new CountDownLatch(2);
        private volatile boolean running;
        private boolean failStop;
        private boolean failCandidateRootCleanup;
        private boolean failMaterializationAfterCreatingRoot;
        private boolean requireConcurrentValidation;
        private volatile boolean stoppedDuringValidation;
        private long validationDelayMillis;
        private volatile int startCount;
        private volatile int stopCount;
        private volatile int shutdownCount;

        private FakeSandbox(ExecResult validationResult) {
            this.validationResult = validationResult;
        }

        @Override
        public synchronized void start() {
            startCount++;
            running = true;
        }

        @Override
        public synchronized void stop() {
            stopCount++;
            if (validationInFlight.get() > 0) {
                stoppedDuringValidation = true;
            }
            running = false;
            if (failStop) {
                throw new IllegalStateException("cleanup failed");
            }
        }

        @Override
        public synchronized void shutdown() {
            shutdownCount++;
        }

        @Override
        public void close() {
            stop();
            shutdown();
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public String getWorkspaceRoot() {
            return "/workspace/test";
        }

        @Override
        public SandboxState getState() {
            return null;
        }

        @Override
        public ExecResult exec(
                RuntimeContext runtimeContext, String command, Integer timeoutSeconds)
                throws Exception {
            commands.add(command);
            if (isMaterializeCommand(command)) {
                String root = candidateRoot(command);
                if (!materializedRoots.add(root)) {
                    return new ExecResult(
                            73, "", "candidate validation root already exists", false);
                }
                if (failMaterializationAfterCreatingRoot) {
                    throw new IllegalStateException("materialization transport failed");
                }
                return new ExecResult(0, "", "", false);
            }
            if (isCleanupCommand(command)) {
                cleanupAttempts.incrementAndGet();
                String root = candidateRoot(command);
                if (failCandidateRootCleanup) {
                    throw new IllegalStateException("candidate cleanup failed");
                }
                materializedRoots.remove(root);
                cleanedRoots.add(root);
                return new ExecResult(0, "", "", false);
            }
            if (isValidationCommand(command)) {
                String root = candidateRoot(command);
                if (!materializedRoots.contains(root)) {
                    return new ExecResult(74, "", "candidate validation root missing", false);
                }
                int sequence = validationSequence.incrementAndGet();
                validationInFlight.incrementAndGet();
                try {
                    if (requireConcurrentValidation) {
                        concurrentValidationEntered.countDown();
                        if (!concurrentValidationEntered.await(1, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("validations did not overlap");
                        }
                        if (sequence == 2) {
                            Thread.sleep(200);
                        }
                    }
                    if (validationDelayMillis > 0) {
                        Thread.sleep(validationDelayMillis);
                    }
                    return validationResult;
                } finally {
                    validationInFlight.decrementAndGet();
                }
            }
            throw new IllegalArgumentException("unexpected sandbox command: " + command);
        }

        private List<String> materializeCommands() {
            return commands.stream().filter(FakeSandbox::isMaterializeCommand).toList();
        }

        private List<String> validationCommands() {
            return commands.stream().filter(FakeSandbox::isValidationCommand).toList();
        }

        private static boolean isMaterializeCommand(String command) {
            return command.startsWith("set -eu; umask 077;");
        }

        private static boolean isValidationCommand(String command) {
            return command.contains("export SKILL_CANDIDATE_ROOT=");
        }

        private static boolean isCleanupCommand(String command) {
            return command.contains("elif [ -e \"$candidate_root\" ]");
        }

        @Override
        public InputStream persistWorkspace() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public void hydrateWorkspace(InputStream archive) {}
    }
}
