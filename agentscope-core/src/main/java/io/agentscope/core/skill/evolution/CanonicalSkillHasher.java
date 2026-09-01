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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Package-owned canonicalization shared by all candidate construction paths. */
final class CanonicalSkillHasher {

    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");
    private static final int MAX_KEY_LENGTH = 128;
    private static final int MAX_NESTING_DEPTH = 64;
    private static final List<String> PLATFORM_KEY_FRAGMENTS =
            List.of(
                    "taskid",
                    "workspaceid",
                    "leaseowner",
                    "leaseepoch",
                    "fencing",
                    "approval",
                    "authorization",
                    "publish",
                    "releasepointer",
                    "routingscope");

    private CanonicalSkillHasher() {}

    static String hash(AgentSkill skill) {
        return hashMaterialized(SkillArtifactMaterializer.materialize(immutableSnapshot(skill)));
    }

    static String hashMaterialized(Map<String, byte[]> files) {
        Objects.requireNonNull(files, "files must not be null");
        MessageDigest digest = sha256();
        update(digest, SkillArtifactHasher.ALGORITHM.getBytes(StandardCharsets.UTF_8));
        files.forEach(
                (path, content) -> {
                    update(
                            digest,
                            requireText(path, "artifact file path")
                                    .getBytes(StandardCharsets.UTF_8));
                    update(
                            digest,
                            Objects.requireNonNull(
                                    content, "artifact file content must not be null"));
                });
        return hex(digest.digest());
    }

    static AgentSkill immutableSnapshot(AgentSkill skill) {
        Objects.requireNonNull(skill, "skill must not be null");
        Map<String, Object> metadata =
                SkillArtifactMaterializer.canonicalMetadata(skill.getMetadata());
        return new AgentSkill(
                metadata,
                skill.getSkillContent(),
                skill.getResources(),
                skill.getSource(),
                skill.getOriginDir().orElse(null));
    }

    static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    static String requireHash(String value, String name) {
        String normalized = requireText(value, name).toLowerCase(Locale.ROOT);
        if (!SHA_256.matcher(normalized).matches()) {
            throw new IllegalArgumentException(name + " must be a lowercase SHA-256 hash");
        }
        return normalized;
    }

    static Map<String, Object> immutableJsonMap(
            Map<String, Object> value, String name, boolean requireSchemaVersion) {
        return immutableJsonMap(
                value,
                name,
                requireSchemaVersion,
                Collections.newSetFromMap(new IdentityHashMap<>()),
                0);
    }

    private static Map<String, Object> immutableJsonMap(
            Map<?, ?> value,
            String name,
            boolean requireSchemaVersion,
            Set<Object> containersBeingCopied,
            int depth) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        requireNestingDepth(depth, name);
        if (!containersBeingCopied.add(value)) {
            throw new IllegalArgumentException(name + " must not contain cycles");
        }
        try {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : value.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException(name + " keys must be strings");
                }
                String validatedKey = requireJsonKey(key, name + " key");
                result.put(
                        validatedKey,
                        immutableJsonValue(
                                entry.getValue(),
                                name + "." + validatedKey,
                                containersBeingCopied,
                                depth + 1));
            }
            if (requireSchemaVersion && !result.containsKey("schemaVersion")) {
                throw new IllegalArgumentException(name + " must contain schemaVersion");
            }
            return Collections.unmodifiableMap(result);
        } finally {
            containersBeingCopied.remove(value);
        }
    }

    static List<Map<String, Object>> immutableJsonMaps(
            List<Map<String, Object>> values, String name) {
        if (values == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        List<Map<String, Object>> result = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.add(immutableJsonMap(values.get(i), name + "[" + i + "]", true));
        }
        return List.copyOf(result);
    }

    private static Object immutableJsonValue(
            Object value, String name, Set<Object> containersBeingCopied, int depth) {
        requireNestingDepth(depth, name);
        if (value == null || value instanceof String || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Number number) {
            return immutableJsonNumber(number, name);
        }
        if (value instanceof Map<?, ?> map) {
            return immutableJsonMap(map, name, false, containersBeingCopied, depth);
        }
        if (value instanceof List<?> list) {
            if (!containersBeingCopied.add(list)) {
                throw new IllegalArgumentException(name + " must not contain cycles");
            }
            try {
                List<Object> copy = new ArrayList<>(list.size());
                for (int i = 0; i < list.size(); i++) {
                    copy.add(
                            immutableJsonValue(
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
        throw new IllegalArgumentException(name + " is not JSON-compatible");
    }

    private static void requireNestingDepth(int depth, String name) {
        if (depth > MAX_NESTING_DEPTH) {
            throw new IllegalArgumentException(
                    name + " exceeds maximum nesting depth " + MAX_NESTING_DEPTH);
        }
    }

    private static String requireJsonKey(String key, String name) {
        String validated = requireText(key, name);
        if (!validated.equals(key)) {
            throw new IllegalArgumentException(name + " must not have surrounding whitespace");
        }
        if (validated.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException(name + " is too long");
        }
        rejectPlatformKey(validated);
        return validated;
    }

    private static Number immutableJsonNumber(Number number, String name) {
        Objects.requireNonNull(number, name + " must not be null");
        if (number instanceof BigDecimal
                || number instanceof BigInteger
                || number instanceof Byte
                || number instanceof Short
                || number instanceof Integer
                || number instanceof Long) {
            return number;
        }
        if (number instanceof Float || number instanceof Double) {
            requireFinite(number, name);
            return number;
        }
        try {
            return new BigDecimal(number.toString());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    name + " is not a JSON-compatible number", exception);
        }
    }

    static void requireFinite(Number number, String name) {
        Objects.requireNonNull(number, name + " must not be null");
        double value = number.doubleValue();
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(name + " must be finite");
        }
    }

    private static void rejectPlatformKey(String key) {
        String normalized = key.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
        for (String fragment : PLATFORM_KEY_FRAGMENTS) {
            if (normalized.contains(fragment)) {
                throw new IllegalArgumentException(
                        "platform orchestration field is not allowed: " + key);
            }
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static void update(MessageDigest digest, byte[] bytes) {
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }
}
