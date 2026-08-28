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

import io.agentscope.core.skill.AgentSkill;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Complete, platform-neutral input for creating or patching a candidate. */
public record SkillCandidateGenerationRequest(
        SkillEvolutionType type,
        List<SkillRevisionRef> sourceRefs,
        List<AgentSkill> sourceSkills,
        Optional<SkillCandidateArtifact> previousCandidate,
        List<SkillEvolutionPayload> disclosedEvidence,
        Optional<SkillEvolutionPayload> disclosedFeedback,
        int iteration,
        SkillEvolutionPayload constraints) {

    public SkillCandidateGenerationRequest {
        type = Objects.requireNonNull(type, "type must not be null");
        if (type == SkillEvolutionType.RETIRE) {
            throw new IllegalArgumentException("RETIRE is not a candidate generation operation");
        }
        sourceRefs = sourceRefs == null ? null : List.copyOf(sourceRefs);
        sourceSkills = sourceSkills == null ? null : List.copyOf(sourceSkills);
        if (sourceRefs == null
                || sourceSkills == null
                || sourceRefs.stream().anyMatch(Objects::isNull)
                || sourceSkills.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("sourceRefs and sourceSkills must not contain null");
        }
        if (sourceRefs.size() != sourceSkills.size()) {
            throw new IllegalArgumentException("sourceRefs and sourceSkills must have equal size");
        }
        validateSourceCount(type, sourceRefs.size());
        previousCandidate = previousCandidate == null ? Optional.empty() : previousCandidate;
        if (iteration < 0) {
            throw new IllegalArgumentException("iteration must not be negative");
        }
        if (iteration == 0 && previousCandidate.isPresent()) {
            throw new IllegalArgumentException(
                    "initial generation must not have previousCandidate");
        }
        if (iteration > 0
                && (previousCandidate.isEmpty()
                        || previousCandidate.get().iteration() >= iteration
                        || previousCandidate.get().type() != type)) {
            throw new IllegalArgumentException(
                    "patch generation requires an earlier candidate of the same type");
        }
        disclosedEvidence = disclosedEvidence == null ? null : List.copyOf(disclosedEvidence);
        if (disclosedEvidence == null || disclosedEvidence.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("disclosedEvidence must not contain null");
        }
        disclosedFeedback = disclosedFeedback == null ? Optional.empty() : disclosedFeedback;
        constraints = Objects.requireNonNull(constraints, "constraints must not be null");
    }

    private static void validateSourceCount(SkillEvolutionType type, int sourceCount) {
        boolean valid =
                switch (type) {
                    case CREATE -> sourceCount == 0;
                    case REFINE -> sourceCount == 1;
                    case MERGE -> sourceCount >= 2;
                    case RETIRE -> false;
                };
        if (!valid) {
            throw new IllegalArgumentException(
                    "source count " + sourceCount + " is invalid for " + type);
        }
    }
}
