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
import java.util.Objects;

/** Canonical, versioned hashing for immutable skill artifacts. */
public final class SkillArtifactHasher {

    /** Canonical hashing algorithm used by the first skill evolution protocol. */
    public static final String ALGORITHM = "agentscope-skill-sha256-v1";

    private SkillArtifactHasher() {}

    /** Returns the canonical hash of SKILL.md and all resources. */
    public static String hash(AgentSkill skill) {
        return CanonicalSkillHasher.hash(skill);
    }

    /** Verifies an algorithm-tagged hash and rejects unknown algorithms. */
    public static boolean verify(AgentSkill skill, String algorithm, String expectedHash) {
        requireSupportedAlgorithm(algorithm);
        return hash(skill).equals(CanonicalSkillHasher.requireHash(expectedHash, "expectedHash"));
    }

    static String requireSupportedAlgorithm(String algorithm) {
        String normalized = CanonicalSkillHasher.requireText(algorithm, "contentHashAlgorithm");
        if (!Objects.equals(ALGORITHM, normalized)) {
            throw new IllegalArgumentException(
                    "unsupported skill content hash algorithm: " + normalized);
        }
        return normalized;
    }
}
