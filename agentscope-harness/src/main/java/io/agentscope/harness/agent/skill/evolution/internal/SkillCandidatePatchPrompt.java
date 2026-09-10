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
package io.agentscope.harness.agent.skill.evolution.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.evolution.SkillCandidateGenerationRequest;
import java.util.LinkedHashMap;
import java.util.Map;

/** Builds the bounded, explicit prompt used by the default candidate generator. */
public final class SkillCandidatePatchPrompt {

    private SkillCandidatePatchPrompt() {}

    /** Serializes disclosed input into a single model instruction. */
    public static String build(SkillCandidateGenerationRequest request, ObjectMapper objectMapper) {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("evolutionType", request.type().name());
        input.put("iteration", request.iteration());
        input.put(
                "sources",
                request.sourceSkills().stream().map(SkillCandidatePatchPrompt::skill).toList());
        input.put("disclosedEvidence", request.disclosedEvidence());
        input.put("constraints", request.constraints());
        request.previousCandidate()
                .ifPresent(
                        candidate -> input.put("previousCandidate", skill(candidate.candidate())));
        request.disclosedFeedback()
                .ifPresent(
                        feedback ->
                                input.put(
                                        "disclosedFeedback",
                                        DevelopmentFeedbackSanitizer.sanitize(feedback)));
        try {
            return "Create the next immutable skill candidate from the disclosed input below. Do"
                    + " not invent hidden evaluation data, platform identifiers, approvals,"
                    + " publishing, or routing. Return JSON only with fields name, description,"
                    + " skillContent, resources.\n"
                    + objectMapper.writeValueAsString(input);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("candidate input cannot be serialized", exception);
        }
    }

    private static Map<String, Object> skill(AgentSkill skill) {
        return Map.of(
                "name", skill.getName(),
                "description", skill.getDescription(),
                "skillContent", skill.getSkillContent(),
                "resources", skill.getResources());
    }
}
