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

import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.evolution.SkillArtifactHasher;
import io.agentscope.core.skill.evolution.SkillCandidateArtifact;
import io.agentscope.core.skill.evolution.SkillCandidateGenerationRequest;
import io.agentscope.core.skill.evolution.SkillEvolutionPayload;
import io.agentscope.core.skill.evolution.SkillEvolutionType;
import io.agentscope.core.skill.evolution.SkillRevisionRef;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class DefaultSkillCandidateGeneratorTest {

    @Test
    void generationIsColdAndProducesIntegrityCheckedCreateCandidate() {
        RecordingModel model = new RecordingModel(candidateJson("created", "new content"));
        DefaultSkillCandidateGenerator generator = new DefaultSkillCandidateGenerator(model);
        SkillCandidateGenerationRequest request = createRequest();

        var result = generator.generate(request);

        assertEquals(0, model.subscriptions.get());
        SkillCandidateArtifact candidate = result.block();
        assertEquals(1, model.subscriptions.get());
        assertEquals("created", candidate.candidate().getName());
        assertTrue(candidate.verifyIntegrity());
        assertTrue(model.lastMessages.get(1).getTextContent().contains("disclosedEvidence"));
    }

    @Test
    void refinePatchCarriesPreviousCandidateAndSanitizedFeedback() {
        AgentSkill sourceSkill = new AgentSkill("source", "source skill", "old", Map.of());
        SkillRevisionRef sourceRef = revisionRef();
        SkillCandidateArtifact previous =
                SkillCandidateArtifact.create(
                        SkillEvolutionType.REFINE, List.of(sourceRef), sourceSkill, 0);
        RecordingModel model = new RecordingModel(candidateJson("source", "fixed"));
        DefaultSkillCandidateGenerator generator = new DefaultSkillCandidateGenerator(model);
        SkillCandidateGenerationRequest request =
                new SkillCandidateGenerationRequest(
                        SkillEvolutionType.REFINE,
                        List.of(sourceRef),
                        List.of(sourceSkill),
                        Optional.of(previous),
                        List.of(payload("evidence", "failureType", "FORMAT")),
                        Optional.of(payload("feedback", "suggestion", "repair")),
                        1,
                        payload("constraints", "language", "zh-CN"));

        SkillCandidateArtifact patched = generator.generate(request).block();

        assertEquals(1, patched.iteration());
        assertEquals("fixed", patched.candidate().getSkillContent());
        String prompt = model.lastMessages.get(1).getTextContent();
        assertTrue(prompt.contains("previousCandidate"));
        assertTrue(prompt.contains("disclosedFeedback"));
    }

    @Test
    void configuredIterationAndOutputLimitsAreHardStops() {
        RecordingModel model = new RecordingModel(candidateJson("created", "new content"));
        DefaultSkillCandidateGenerator generator =
                new DefaultSkillCandidateGenerator(
                        model, new com.fasterxml.jackson.databind.ObjectMapper(), 1, 20);

        assertThrows(
                IllegalArgumentException.class, () -> generator.generate(createRequest()).block());

        SkillCandidateGenerationRequest iterationOne =
                new SkillCandidateGenerationRequest(
                        SkillEvolutionType.REFINE,
                        List.of(revisionRef()),
                        List.of(new AgentSkill("source", "source skill", "old", Map.of())),
                        Optional.of(
                                SkillCandidateArtifact.create(
                                        SkillEvolutionType.REFINE,
                                        List.of(revisionRef()),
                                        new AgentSkill("source", "source skill", "old", Map.of()),
                                        0)),
                        List.of(),
                        Optional.empty(),
                        1,
                        payload("constraints", "language", "zh-CN"));
        assertThrows(
                IllegalArgumentException.class, () -> generator.generate(iterationOne).block());
    }

    private static SkillCandidateGenerationRequest createRequest() {
        return new SkillCandidateGenerationRequest(
                SkillEvolutionType.CREATE,
                List.of(),
                List.of(),
                Optional.empty(),
                List.of(payload("evidence", "failureType", "MISSING_FORMAT")),
                Optional.empty(),
                0,
                payload("constraints", "language", "zh-CN"));
    }

    private static SkillEvolutionPayload payload(String schema, String key, Object value) {
        return SkillEvolutionPayload.versionOne(
                "test.skill-evolution." + schema, Map.of(key, value));
    }

    private static SkillRevisionRef revisionRef() {
        return new SkillRevisionRef("source", "1", SkillArtifactHasher.ALGORITHM, "c".repeat(64));
    }

    private static String candidateJson(String name, String content) {
        return """
        ```json
        {"name":"%s","description":"generated skill","skillContent":"%s","resources":{}}
        ```
        """
                .formatted(name, content);
    }

    private static final class RecordingModel implements Model {

        private final AtomicInteger subscriptions = new AtomicInteger();
        private final String response;
        private List<Msg> lastMessages = List.of();

        private RecordingModel(String response) {
            this.response = response;
        }

        @Override
        public Flux<ChatResponse> stream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            lastMessages = List.copyOf(messages);
            return Flux.defer(
                    () -> {
                        subscriptions.incrementAndGet();
                        return Flux.just(
                                ChatResponse.builder()
                                        .content(
                                                List.of(TextBlock.builder().text(response).build()))
                                        .build());
                    });
        }

        @Override
        public String getModelName() {
            return "recording-model";
        }
    }
}
