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
package io.agentscope.core.skill.evolution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.skill.AgentSkill;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SkillEvolutionContractTest {

    @Test
    void payloadIsDeeplyImmutableAndVersioned() {
        List<Object> nested = new ArrayList<>();
        nested.add("initial");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("allowed", nested);

        SkillEvolutionPayload payload = new SkillEvolutionPayload("test.payload", 1, data);
        nested.add("changed");

        assertEquals(List.of("initial"), payload.data().get("allowed"));
        assertThrows(UnsupportedOperationException.class, () -> payload.data().put("new", true));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("Invalid Schema", 1, Map.of()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.payload", 0, Map.of()));
    }

    @Test
    void canonicalHashIsStableAcrossLineEndingsAndResourceOrder() {
        AgentSkill first =
                new AgentSkill(
                        "demo",
                        "demo skill",
                        "first\r\nsecond\r\n",
                        linkedResources("b.txt", "two\r\n", "a.txt", "one\n"));
        AgentSkill second =
                new AgentSkill(
                        "demo",
                        "demo skill",
                        "first\nsecond\n",
                        linkedResources("a.txt", "one\n", "b.txt", "two\n"));

        SkillCandidateArtifact firstArtifact =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), first, 0);
        SkillCandidateArtifact secondArtifact =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), second, 0);

        assertEquals(firstArtifact.contentHash(), secondArtifact.contentHash());
        assertTrue(firstArtifact.verifyIntegrity());
    }

    @Test
    void publicConstructorRejectsTamperedContentHash() {
        AgentSkill skill = skill("original");
        String validHash =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), skill, 0)
                        .contentHash();
        AgentSkill changed = skill("changed");

        assertNotEquals(
                validHash,
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), changed, 0)
                        .contentHash());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillCandidateArtifact(
                                SkillEvolutionType.CREATE,
                                List.of(),
                                changed,
                                SkillArtifactHasher.ALGORITHM,
                                validHash,
                                0));
    }

    @Test
    void canonicalHashRejectsResourcesOutsideCandidateRoot() {
        AgentSkill skill =
                new AgentSkill("demo", "demo skill", "content", Map.of("../escape.txt", "blocked"));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillCandidateArtifact.create(
                                SkillEvolutionType.CREATE, List.of(), skill, 0));
    }

    @Test
    void sourceCardinalityAndPatchIterationAreEnforced() {
        SkillRevisionRef source = revision("source");
        AgentSkill sourceSkill = skill("source");
        SkillEvolutionPayload constraints = payload("constraints");

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillCandidateGenerationRequest(
                                SkillEvolutionType.REFINE,
                                List.of(),
                                List.of(),
                                Optional.empty(),
                                List.of(),
                                Optional.empty(),
                                0,
                                constraints));

        SkillCandidateArtifact previous =
                SkillCandidateArtifact.create(
                        SkillEvolutionType.REFINE, List.of(source), sourceSkill, 0);
        SkillCandidateGenerationRequest patch =
                new SkillCandidateGenerationRequest(
                        SkillEvolutionType.REFINE,
                        List.of(source),
                        List.of(sourceSkill),
                        Optional.of(previous),
                        List.of(payload("evidence")),
                        Optional.of(payload("feedback")),
                        1,
                        constraints);

        assertEquals(1, patch.iteration());
        assertEquals(previous, patch.previousCandidate().orElseThrow());
    }

    @Test
    void requestPayloadsRejectPlatformFields() {
        List<Object> nested = new ArrayList<>();
        nested.add("initial");
        Map<String, Object> constraintData = new LinkedHashMap<>();
        constraintData.put("allowed", nested);
        SkillCandidateGenerationRequest request =
                new SkillCandidateGenerationRequest(
                        SkillEvolutionType.CREATE,
                        List.of(),
                        List.of(),
                        Optional.empty(),
                        List.of(),
                        Optional.empty(),
                        0,
                        new SkillEvolutionPayload("test.constraints", 1, constraintData));
        nested.add("changed");

        assertEquals(List.of("initial"), request.constraints().data().get("allowed"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> request.constraints().data().put("new", true));

        Map<String, Object> invalid = new LinkedHashMap<>();
        invalid.put("workspace_id", "forbidden");
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.constraints", 1, invalid));
    }

    @Test
    void finalGateAlwaysRemovesDisclosedFeedback() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillValidationReport(
                                SkillValidationStage.FINAL_GATE,
                                false,
                                "a".repeat(64),
                                metrics(0.4),
                                Optional.of(payload("feedback"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillValidationReport(
                                SkillValidationStage.DEVELOPMENT,
                                false,
                                "b".repeat(64),
                                new SkillEvolutionPayload(
                                        "test.metrics", 1, Map.of("score", "not-a-number")),
                                Optional.empty()));
    }

    @Test
    void validationRequestRequiresPositiveTimeoutAndSchemaVersion() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillValidationRequest(
                                SkillValidationStage.DEVELOPMENT,
                                payload("validation-spec"),
                                payload("validation-constraints"),
                                Duration.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.payload", 1, Map.of("score", Double.NaN)));
    }

    private static AgentSkill skill(String content) {
        return new AgentSkill("demo", "demo skill", content, Map.of());
    }

    private static SkillRevisionRef revision(String skillId) {
        return new SkillRevisionRef(skillId, "1", SkillArtifactHasher.ALGORITHM, "c".repeat(64));
    }

    private static SkillEvolutionPayload payload(String name) {
        return SkillEvolutionPayload.versionOne("test." + name, Map.of());
    }

    private static SkillEvolutionPayload metrics(double score) {
        return SkillEvolutionPayload.versionOne("test.metrics", Map.of("score", score));
    }

    private static Map<String, String> linkedResources(String... values) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put(values[i], values[i + 1]);
        }
        return result;
    }
}
