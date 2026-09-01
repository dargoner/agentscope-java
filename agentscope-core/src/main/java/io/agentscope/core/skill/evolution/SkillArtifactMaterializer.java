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
import io.agentscope.core.skill.util.MarkdownSkillParser;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Materializes an {@link AgentSkill} into the canonical files used for hashing and validation. */
public final class SkillArtifactMaterializer {

    /** Canonical path of the skill descriptor inside every materialized artifact. */
    public static final String SKILL_FILE = "SKILL.md";

    private static final int MAX_NESTING_DEPTH = 64;

    private SkillArtifactMaterializer() {}

    /**
     * Returns a path-sorted canonical UTF-8 file mapping.
     *
     * <p>The returned map is unmodifiable. Each invocation owns its byte arrays, so mutation of an
     * array returned by one invocation cannot affect later materialization or hashing.
     *
     * @throws IllegalArgumentException if metadata is not canonicalizable, a resource escapes the
     *     artifact, aliases another resource, conflicts with a resource directory, or masquerades
     *     as {@value #SKILL_FILE}
     */
    public static Map<String, byte[]> materialize(AgentSkill skill) {
        Objects.requireNonNull(skill, "skill must not be null");
        Map<String, Object> metadata = canonicalMetadata(skill.getMetadata());
        String skillMarkdown =
                normalizeText(
                        MarkdownSkillParser.generate(
                                metadata, normalizeText(skill.getSkillContent())));

        TreeMap<String, byte[]> files = new TreeMap<>();
        files.put(SKILL_FILE, utf8(skillMarkdown));
        for (Map.Entry<String, String> resource : skill.getResources().entrySet()) {
            String path = normalizeResourcePath(resource.getKey());
            rejectSkillFileAlias(path);
            rejectPathConflict(files.keySet(), path);
            if (files.putIfAbsent(path, utf8(normalizeText(resource.getValue()))) != null) {
                throw new IllegalArgumentException(
                        "resource paths collide after normalization: " + path);
            }
        }
        return Collections.unmodifiableMap(files);
    }

    static Map<String, Object> canonicalMetadata(Map<?, ?> metadata) {
        Objects.requireNonNull(metadata, "metadata must not be null");
        return canonicalMetadata(
                metadata, "metadata", Collections.newSetFromMap(new IdentityHashMap<>()), 0);
    }

    private static Map<String, Object> canonicalMetadata(
            Map<?, ?> metadata, String name, Set<Object> containersBeingCopied, int depth) {
        requireNestingDepth(depth, name);
        if (!containersBeingCopied.add(metadata)) {
            throw new IllegalArgumentException(name + " must not contain cycles");
        }
        try {
            TreeMap<String, Object> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> entry : metadata.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException(name + " keys must be strings");
                }
                String canonicalKey = normalizeText(key);
                if (sorted.containsKey(canonicalKey)) {
                    throw new IllegalArgumentException(
                            name + " keys collide after normalization: " + canonicalKey);
                }
                sorted.put(
                        canonicalKey,
                        canonicalMetadataValue(
                                entry.getValue(),
                                name + "." + canonicalKey,
                                containersBeingCopied,
                                depth + 1));
            }
            return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
        } finally {
            containersBeingCopied.remove(metadata);
        }
    }

    private static Object canonicalMetadataValue(
            Object value, String name, Set<Object> containersBeingCopied, int depth) {
        requireNestingDepth(depth, name);
        if (value == null || value instanceof Boolean) {
            return value;
        }
        if (value instanceof String string) {
            return normalizeText(string);
        }
        if (value instanceof Number number) {
            return canonicalNumber(number, name);
        }
        if (value instanceof Date date) {
            return new ImmutableDate(date.getTime());
        }
        if (value instanceof Map<?, ?> map) {
            return canonicalMetadata(map, name, containersBeingCopied, depth);
        }
        if (value instanceof List<?> list) {
            if (!containersBeingCopied.add(list)) {
                throw new IllegalArgumentException(name + " must not contain cycles");
            }
            try {
                List<Object> copy = new ArrayList<>(list.size());
                for (int i = 0; i < list.size(); i++) {
                    copy.add(
                            canonicalMetadataValue(
                                    list.get(i),
                                    name + "[" + i + "]",
                                    containersBeingCopied,
                                    depth + 1));
                }
                return Collections.unmodifiableList(copy);
            } finally {
                containersBeingCopied.remove(list);
            }
        }
        throw new IllegalArgumentException(name + " is not canonical metadata");
    }

    private static Number canonicalNumber(Number number, String name) {
        if (number instanceof BigInteger bigInteger) {
            return bigInteger;
        }
        if (number instanceof Byte
                || number instanceof Short
                || number instanceof Integer
                || number instanceof Long) {
            return BigInteger.valueOf(number.longValue());
        }

        BigDecimal decimal;
        if (number instanceof BigDecimal bigDecimal) {
            decimal = bigDecimal;
        } else if (number instanceof Float || number instanceof Double) {
            CanonicalSkillHasher.requireFinite(number, name);
            decimal = BigDecimal.valueOf(number.doubleValue());
        } else {
            try {
                String text = number.toString();
                if (text.matches("[+-]?[0-9]+")) {
                    return new BigInteger(text);
                }
                decimal = new BigDecimal(text);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(name + " is not a canonical number", exception);
            }
        }
        return decimal.signum() == 0 ? BigDecimal.ZERO : decimal.stripTrailingZeros();
    }

    private static String normalizeResourcePath(String value) {
        rejectControlCharacters(value);
        String path =
                Normalizer.normalize(
                                CanonicalSkillHasher.requireText(value, "resource path"),
                                Normalizer.Form.NFC)
                        .replace('\\', '/');
        if (path.startsWith("/") || path.matches("^[A-Za-z]:/.*")) {
            throw new IllegalArgumentException("resource path must remain inside the artifact");
        }
        List<String> segments = new ArrayList<>();
        for (String segment : path.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".")) {
                continue;
            }
            if (segment.equals("..")) {
                throw new IllegalArgumentException("resource path must remain inside the artifact");
            }
            segments.add(segment);
        }
        if (segments.isEmpty()) {
            throw new IllegalArgumentException("resource path must identify a file");
        }
        return String.join("/", segments);
    }

    private static void rejectSkillFileAlias(String path) {
        String firstSegment = path.split("/", 2)[0];
        if (collisionKey(firstSegment).equals(collisionKey(SKILL_FILE))) {
            throw new IllegalArgumentException(
                    "resource path must not masquerade as " + SKILL_FILE);
        }
    }

    private static void rejectPathConflict(Set<String> existingPaths, String candidate) {
        String candidateKey = collisionKey(candidate);
        for (String existing : existingPaths) {
            if (existing.equals(SKILL_FILE)) {
                continue;
            }
            String existingKey = collisionKey(existing);
            if (existingKey.equals(candidateKey)) {
                throw new IllegalArgumentException(
                        "resource paths collide after normalization: " + candidate);
            }
            if (existingKey.startsWith(candidateKey + "/")
                    || candidateKey.startsWith(existingKey + "/")) {
                throw new IllegalArgumentException(
                        "resource path conflicts with a resource directory: " + candidate);
            }
        }
    }

    private static void rejectControlCharacters(String path) {
        if (path == null) {
            throw new IllegalArgumentException("resource path must not be blank");
        }
        if (path.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("resource path must not contain control characters");
        }
    }

    private static String collisionKey(String path) {
        String normalized = Normalizer.normalize(path, Normalizer.Form.NFC);
        String portableCaseFold = normalized.toUpperCase(Locale.ROOT).toLowerCase(Locale.ROOT);
        return Normalizer.normalize(portableCaseFold, Normalizer.Form.NFC);
    }

    private static void requireNestingDepth(int depth, String name) {
        if (depth > MAX_NESTING_DEPTH) {
            throw new IllegalArgumentException(
                    name + " exceeds maximum nesting depth " + MAX_NESTING_DEPTH);
        }
    }

    private static String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\r\n", "\n").replace('\r', '\n');
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** Immutable Date subtype so parsed YAML timestamps remain timestamps without mutable aliases. */
    private static final class ImmutableDate extends Date {

        private ImmutableDate(long time) {
            super(time);
        }

        @Override
        public void setTime(long time) {
            throw new UnsupportedOperationException("canonical metadata dates are immutable");
        }

        @Override
        @Deprecated
        public void setYear(int year) {
            throw new UnsupportedOperationException("canonical metadata dates are immutable");
        }

        @Override
        @Deprecated
        public void setMonth(int month) {
            throw new UnsupportedOperationException("canonical metadata dates are immutable");
        }

        @Override
        @Deprecated
        public void setDate(int date) {
            throw new UnsupportedOperationException("canonical metadata dates are immutable");
        }

        @Override
        @Deprecated
        public void setHours(int hours) {
            throw new UnsupportedOperationException("canonical metadata dates are immutable");
        }

        @Override
        @Deprecated
        public void setMinutes(int minutes) {
            throw new UnsupportedOperationException("canonical metadata dates are immutable");
        }

        @Override
        @Deprecated
        public void setSeconds(int seconds) {
            throw new UnsupportedOperationException("canonical metadata dates are immutable");
        }

        @Override
        public Object clone() {
            return new ImmutableDate(getTime());
        }
    }
}
