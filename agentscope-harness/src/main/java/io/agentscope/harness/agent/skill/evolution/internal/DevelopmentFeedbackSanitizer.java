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

import io.agentscope.core.skill.evolution.SkillEvolutionPayload;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Bounds development feedback before it is included in a candidate patch prompt. */
public final class DevelopmentFeedbackSanitizer {

    private static final int MAX_ENTRIES = 50;
    private static final int MAX_TEXT_LENGTH = 4_096;

    private DevelopmentFeedbackSanitizer() {}

    /** Returns a bounded copy without changing the caller-owned value. */
    public static SkillEvolutionPayload sanitize(SkillEvolutionPayload feedback) {
        if (feedback == null || feedback.data().isEmpty()) {
            return SkillEvolutionPayload.versionOne(
                    "agentscope.skill-evolution.feedback", Map.of());
        }
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        feedback.data().entrySet().stream()
                .limit(MAX_ENTRIES)
                .forEach(entry -> result.put(entry.getKey(), sanitizeValue(entry.getValue())));
        return new SkillEvolutionPayload(
                feedback.schemaId(), feedback.schemaVersion(), Map.copyOf(result));
    }

    private static Object sanitizeValue(Object value) {
        if (value instanceof String text) {
            return text.length() <= MAX_TEXT_LENGTH
                    ? text
                    : text.substring(0, MAX_TEXT_LENGTH) + "…";
        }
        if (value instanceof List<?> list) {
            return list.stream()
                    .limit(MAX_ENTRIES)
                    .map(DevelopmentFeedbackSanitizer::sanitizeValue)
                    .toList();
        }
        if (value instanceof Map<?, ?> map) {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            map.entrySet().stream()
                    .filter(entry -> entry.getKey() instanceof String)
                    .limit(MAX_ENTRIES)
                    .forEach(
                            entry ->
                                    result.put(
                                            (String) entry.getKey(),
                                            sanitizeValue(entry.getValue())));
            return Map.copyOf(result);
        }
        return value;
    }
}
