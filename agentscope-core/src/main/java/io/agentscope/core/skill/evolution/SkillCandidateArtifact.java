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

/** Immutable, integrity-checked skill candidate. */
public record SkillCandidateArtifact(
        SkillEvolutionType type,
        List<SkillRevisionRef> sources,
        AgentSkill candidate,
        String contentHash,
        int iteration) {

    public SkillCandidateArtifact {
        type = Objects.requireNonNull(type, "type must not be null");
        if (type == SkillEvolutionType.RETIRE) {
            throw new IllegalArgumentException("RETIRE does not produce a candidate artifact");
        }
        sources = sources == null ? null : List.copyOf(sources);
        if (sources == null || sources.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("sources must not contain null");
        }
        validateSourceCount(type, sources.size());
        candidate = Objects.requireNonNull(candidate, "candidate must not be null");
        if (iteration < 0) {
            throw new IllegalArgumentException("iteration must not be negative");
        }
        contentHash = CanonicalSkillHasher.requireHash(contentHash, "contentHash");
        String actualHash = CanonicalSkillHasher.hash(candidate);
        if (!actualHash.equals(contentHash)) {
            throw new IllegalArgumentException("contentHash does not match candidate content");
        }
    }

    /** Creates an artifact and computes its canonical content hash. */
    public static SkillCandidateArtifact create(
            SkillEvolutionType type,
            List<SkillRevisionRef> sources,
            AgentSkill candidate,
            int iteration) {
        return new SkillCandidateArtifact(
                type, sources, candidate, CanonicalSkillHasher.hash(candidate), iteration);
    }

    /** Recomputes and verifies the canonical content hash. */
    public boolean verifyIntegrity() {
        return contentHash.equals(CanonicalSkillHasher.hash(candidate));
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
