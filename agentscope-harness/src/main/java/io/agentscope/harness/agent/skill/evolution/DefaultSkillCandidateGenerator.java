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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.SystemMessage;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.UserMessage;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.evolution.SkillCandidateArtifact;
import io.agentscope.core.skill.evolution.SkillCandidateGenerationRequest;
import io.agentscope.core.skill.evolution.SkillCandidateGenerator;
import io.agentscope.harness.agent.skill.evolution.internal.SkillCandidatePatchPrompt;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import reactor.core.publisher.Mono;

/** Default model-backed CREATE/REFINE candidate generator. */
public final class DefaultSkillCandidateGenerator implements SkillCandidateGenerator {

    private static final int DEFAULT_MAX_ITERATIONS = 3;
    private static final int DEFAULT_MAX_OUTPUT_CHARACTERS = 1_000_000;
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final String SYSTEM_PROMPT =
            "You create skill candidates only. Never publish, route, approve, or expose hidden"
                    + " evaluation data.";

    private final Model model;
    private final ObjectMapper objectMapper;
    private final int maxIterations;
    private final int maxOutputCharacters;

    /** Creates a generator with three total candidate iterations. */
    public DefaultSkillCandidateGenerator(Model model) {
        this(model, new ObjectMapper(), DEFAULT_MAX_ITERATIONS, DEFAULT_MAX_OUTPUT_CHARACTERS);
    }

    /** Creates a generator with explicit, testable limits. */
    public DefaultSkillCandidateGenerator(
            Model model, ObjectMapper objectMapper, int maxIterations, int maxOutputCharacters) {
        this.model = Objects.requireNonNull(model, "model must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        if (maxIterations <= 0 || maxOutputCharacters <= 0) {
            throw new IllegalArgumentException("generation limits must be positive");
        }
        this.maxIterations = maxIterations;
        this.maxOutputCharacters = maxOutputCharacters;
    }

    @Override
    public Mono<SkillCandidateArtifact> generate(SkillCandidateGenerationRequest request) {
        return Mono.defer(
                () -> {
                    Objects.requireNonNull(request, "request must not be null");
                    if (request.iteration() >= maxIterations) {
                        return Mono.error(
                                new IllegalArgumentException(
                                        "candidate iteration exceeds configured maximum"));
                    }
                    String prompt = SkillCandidatePatchPrompt.build(request, objectMapper);
                    return model.stream(
                                    List.of(
                                            new SystemMessage(SYSTEM_PROMPT),
                                            new UserMessage(prompt)),
                                    List.of(),
                                    null)
                            .map(DefaultSkillCandidateGenerator::text)
                            .reduce(new StringBuilder(), this::appendBounded)
                            .map(StringBuilder::toString)
                            .map(this::parseSkill)
                            .map(
                                    candidate ->
                                            SkillCandidateArtifact.create(
                                                    request.type(),
                                                    request.sourceRefs(),
                                                    candidate,
                                                    request.iteration()));
                });
    }

    private StringBuilder appendBounded(StringBuilder result, String chunk) {
        if (result.length() + chunk.length() > maxOutputCharacters) {
            throw new IllegalArgumentException("candidate model output exceeds configured limit");
        }
        return result.append(chunk);
    }

    private AgentSkill parseSkill(String rawOutput) {
        String json = stripJsonFence(rawOutput);
        try {
            Map<String, Object> payload = objectMapper.readValue(json, MAP_TYPE);
            String name = requiredString(payload, "name");
            String description = requiredString(payload, "description");
            String skillContent = requiredString(payload, "skillContent");
            Map<String, String> resources =
                    objectMapper.convertValue(
                            payload.getOrDefault("resources", Map.of()),
                            new TypeReference<Map<String, String>>() {});
            return new AgentSkill(name, description, skillContent, resources, "skill-evolution");
        } catch (RuntimeException | com.fasterxml.jackson.core.JsonProcessingException exception) {
            throw new IllegalArgumentException(
                    "candidate model output is not valid skill JSON", exception);
        }
    }

    private static String text(ChatResponse response) {
        if (response == null || response.getContent() == null) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for (ContentBlock block : response.getContent()) {
            if (block instanceof TextBlock textBlock) {
                result.append(textBlock.getText());
            }
        }
        return result.toString();
    }

    private static String stripJsonFence(String value) {
        String trimmed = value == null ? "" : value.trim();
        if (trimmed.startsWith("```")) {
            int firstLine = trimmed.indexOf('\n');
            int lastFence = trimmed.lastIndexOf("```");
            if (firstLine >= 0 && lastFence > firstLine) {
                return trimmed.substring(firstLine + 1, lastFence).trim();
            }
        }
        return trimmed;
    }

    private static String requiredString(Map<String, Object> value, String key) {
        Object item = value.get(key);
        if (!(item instanceof String text) || text.trim().isEmpty()) {
            throw new IllegalArgumentException("candidate field is missing: " + key);
        }
        return text.trim();
    }
}
