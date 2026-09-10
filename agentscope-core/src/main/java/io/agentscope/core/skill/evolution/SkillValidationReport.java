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

import java.util.Objects;
import java.util.Optional;

/** Immutable validation outcome with stage-aware feedback disclosure. */
public record SkillValidationReport(
        SkillValidationStage stage,
        boolean passed,
        String reportHash,
        SkillEvolutionPayload metrics,
        Optional<SkillEvolutionPayload> disclosedFeedback) {

    public SkillValidationReport {
        stage = Objects.requireNonNull(stage, "stage must not be null");
        reportHash = CanonicalSkillHasher.requireHash(reportHash, "reportHash");
        metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        metrics.data()
                .forEach(
                        (key, value) -> {
                            if (!(value instanceof Number number)) {
                                throw new IllegalArgumentException(
                                        "metrics." + key + " must be a number");
                            }
                            CanonicalSkillHasher.requireFinite(number, "metrics." + key);
                        });
        disclosedFeedback = disclosedFeedback == null ? Optional.empty() : disclosedFeedback;
        if (stage == SkillValidationStage.FINAL_GATE && disclosedFeedback.isPresent()) {
            throw new IllegalArgumentException("FINAL_GATE must not disclose feedback");
        }
    }
}
