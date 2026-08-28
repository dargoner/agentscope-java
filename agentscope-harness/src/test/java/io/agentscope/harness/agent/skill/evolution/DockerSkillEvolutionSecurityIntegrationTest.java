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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.evolution.SkillCandidateArtifact;
import io.agentscope.core.skill.evolution.SkillEvolutionType;
import io.agentscope.core.skill.evolution.SkillValidationReport;
import io.agentscope.core.skill.evolution.SkillValidationRequest;
import io.agentscope.core.skill.evolution.SkillValidationStage;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.impl.docker.DockerSandboxClient;
import io.agentscope.harness.agent.sandbox.impl.docker.DockerSandboxClientOptions;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "AGENTSCOPE_SKILL_EVOLUTION_DOCKER", matches = "true")
class DockerSkillEvolutionSecurityIntegrationTest {

    @Test
    void hardenedDockerValidationIsRepeatableAndLeavesNoContainer() throws Exception {
        int containersBefore = sandboxContainerCount();

        for (int run = 0; run < 3; run++) {
            SandboxSkillCandidateValidator validator = ownedValidator();
            SkillValidationReport report =
                    validator
                            .validate(
                                    candidate(),
                                    request(
                                            "test \"$(id -u)\" = 65532 && grep -Eq"
                                                + " 'NoNewPrivs:[[:space:]]*1' /proc/self/status &&"
                                                + " grep -Eq 'CapEff:[[:space:]]*0+$'"
                                                + " /proc/self/status && test ! -S"
                                                + " /var/run/docker.sock && test -f SKILL.md && !"
                                                + " touch /etc/skill-evolution-blocked && ! python"
                                                + " -c \"import socket;"
                                                + " socket.create_connection(('1.1.1.1', 80),"
                                                + " 1)\""))
                            .block();

            assertTrue(report.passed(), "hardened validation run " + run + " must pass");
            assertTrue(report.disclosedFeedback().isEmpty());
        }

        assertEquals(containersBefore, sandboxContainerCount());
    }

    @Test
    void timeoutAndMaliciousPathCannotLeaveOwnedResources() throws Exception {
        int containersBefore = sandboxContainerCount();
        SandboxSkillCandidateValidator validator = ownedValidator();
        SkillValidationRequest timeout =
                new SkillValidationRequest(
                        SkillValidationStage.DEVELOPMENT,
                        Map.of(
                                "schemaVersion", 1,
                                "command", "while :; do :; done",
                                "expectedExitCode", 0),
                        Map.of("schemaVersion", 1),
                        Duration.ofMillis(300));

        assertThrows(
                RuntimeException.class, () -> validator.validate(candidate(), timeout).block());
        assertEquals(containersBefore, sandboxContainerCount());

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillCandidateArtifact.create(
                                SkillEvolutionType.CREATE,
                                List.of(),
                                new AgentSkill(
                                        "malicious",
                                        "malicious path",
                                        "blocked",
                                        Map.of("../escape", "blocked")),
                                0));
        assertEquals(containersBefore, sandboxContainerCount());
    }

    private static SandboxSkillCandidateValidator ownedValidator() {
        WorkspaceSpec workspace = new WorkspaceSpec();
        workspace.setRoot("/workspace");
        DockerSandboxClientOptions options =
                new DockerSandboxClientOptions()
                        .image(
                                "python@sha256:646fb0bca3dd3ea1bcc6feb72c17ed16eed6e10cffc732fcc1478bd3e7f02d7b")
                        .network("none")
                        .memorySizeBytes(256L * 1024L * 1024L)
                        .cpuCount(1L)
                        .additionalRunArgs(
                                "--read-only",
                                "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=16m",
                                "--tmpfs=/workspace:rw,nosuid,nodev,noexec,size=32m,uid=65532,gid=65532,mode=0700",
                                "--user=65532:65532",
                                "--cap-drop=ALL",
                                "--security-opt=no-new-privileges",
                                "--pids-limit=64");
        return SandboxSkillCandidateValidator.owned(
                new DockerSandboxClient(), workspace, null, options);
    }

    private static SkillCandidateArtifact candidate() {
        return SkillCandidateArtifact.create(
                SkillEvolutionType.CREATE,
                List.of(),
                new AgentSkill(
                        "docker-pilot",
                        "docker security pilot",
                        "Validate the isolated candidate.",
                        Map.of("checks/readme.txt", "isolated")),
                0);
    }

    private static SkillValidationRequest request(String command) {
        return new SkillValidationRequest(
                SkillValidationStage.FINAL_GATE,
                Map.of("schemaVersion", 1, "command", command, "expectedExitCode", 0),
                Map.of("schemaVersion", 1),
                Duration.ofSeconds(15));
    }

    private static int sandboxContainerCount() throws Exception {
        Process process =
                new ProcessBuilder(
                                "docker",
                                "ps",
                                "-a",
                                "--filter",
                                "name=agentscope-sandbox-",
                                "--format",
                                "{{.ID}}")
                        .start();
        int count;
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            count = (int) reader.lines().filter(line -> !line.isBlank()).count();
        }
        if (process.waitFor() != 0) {
            throw new IllegalStateException("docker container inventory failed");
        }
        return count;
    }
}
