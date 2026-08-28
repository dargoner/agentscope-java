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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable validation outcome with stage-aware feedback disclosure. */
public record SkillValidationReport(
        SkillValidationStage stage,
        boolean passed,
        String reportHash,
        Map<String, Number> metrics,
        Optional<Map<String, Object>> disclosedFeedback) {

    public SkillValidationReport {
        stage = Objects.requireNonNull(stage, "stage must not be null");
        reportHash = CanonicalSkillHasher.requireHash(reportHash, "reportHash");
        if (metrics == null) {
            throw new IllegalArgumentException("metrics must not be null");
        }
        LinkedHashMap<String, Number> metricsCopy = new LinkedHashMap<>();
        metrics.forEach(
                (key, value) -> {
                    String normalizedKey = CanonicalSkillHasher.requireText(key, "metrics key");
                    if (normalizedKey.length() > 128) {
                        throw new IllegalArgumentException("metrics key is too long");
                    }
                    CanonicalSkillHasher.requireFinite(value, "metrics." + normalizedKey);
                    metricsCopy.put(normalizedKey, value);
                });
        metrics = Collections.unmodifiableMap(metricsCopy);
        disclosedFeedback =
                disclosedFeedback == null
                        ? Optional.empty()
                        : disclosedFeedback.map(
                                value ->
                                        CanonicalSkillHasher.immutableJsonMap(
                                                value, "disclosedFeedback", true));
        if (stage == SkillValidationStage.FINAL_GATE) {
            disclosedFeedback = Optional.empty();
        }
    }
}
