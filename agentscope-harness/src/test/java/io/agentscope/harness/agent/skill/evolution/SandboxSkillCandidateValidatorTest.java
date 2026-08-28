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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.evolution.SkillCandidateArtifact;
import io.agentscope.core.skill.evolution.SkillEvolutionType;
import io.agentscope.core.skill.evolution.SkillValidationReport;
import io.agentscope.core.skill.evolution.SkillValidationRequest;
import io.agentscope.core.skill.evolution.SkillValidationStage;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxState;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
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
    void timeoutStopsExecutionAndCleanupFailureIsPropagated() {
        FakeSandbox slow = new FakeSandbox(new ExecResult(0, "passed", "", false));
        slow.validationDelayMillis = 2_000;
        SandboxSkillCandidateValidator timeoutValidator =
                new SandboxSkillCandidateValidator(
                        () -> slow, false, Schedulers.boundedElastic(), 512);
        SkillValidationRequest timeoutRequest =
                new SkillValidationRequest(
                        SkillValidationStage.DEVELOPMENT,
                        Map.of("schemaVersion", 1, "command", "verify"),
                        Map.of("schemaVersion", 1),
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
        return SkillCandidateArtifact.create(
                SkillEvolutionType.CREATE,
                List.of(),
                new AgentSkill(
                        "demo",
                        "demo skill",
                        "instructions",
                        Map.of("scripts/verify.sh", "echo passed")),
                0);
    }

    private static SkillValidationRequest request(SkillValidationStage stage) {
        return new SkillValidationRequest(
                stage,
                Map.of(
                        "schemaVersion", 1,
                        "command", "test -f SKILL.md && echo passed",
                        "expectedExitCode", 0),
                Map.of("schemaVersion", 1),
                Duration.ofSeconds(2));
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage();
    }

    private static final class FakeSandbox implements Sandbox {

        private final ExecResult validationResult;
        private final List<String> commands = new ArrayList<>();
        private boolean running;
        private boolean failStop;
        private long validationDelayMillis;
        private int startCount;
        private int stopCount;
        private int shutdownCount;

        private FakeSandbox(ExecResult validationResult) {
            this.validationResult = validationResult;
        }

        @Override
        public void start() {
            startCount++;
            running = true;
        }

        @Override
        public void stop() {
            stopCount++;
            running = false;
            if (failStop) {
                throw new IllegalStateException("cleanup failed");
            }
        }

        @Override
        public void shutdown() {
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
            if (commands.size() == 1) {
                return new ExecResult(0, "", "", false);
            }
            if (validationDelayMillis > 0) {
                Thread.sleep(validationDelayMillis);
            }
            return validationResult;
        }

        @Override
        public InputStream persistWorkspace() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public void hydrateWorkspace(InputStream archive) {}
    }
}
