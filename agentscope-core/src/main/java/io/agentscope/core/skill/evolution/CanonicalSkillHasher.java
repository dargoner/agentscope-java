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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Package-owned canonicalization shared by all candidate construction paths. */
final class CanonicalSkillHasher {

    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");
    private static final int MAX_KEY_LENGTH = 128;
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
        Objects.requireNonNull(skill, "candidate must not be null");
        MessageDigest digest = sha256();
        update(digest, "skill-content");
        update(digest, normalizeText(skill.getSkillContent()));
        TreeMap<String, String> resources = new TreeMap<>();
        skill.getResources()
                .forEach(
                        (path, content) ->
                                resources.put(normalizePath(path), normalizeText(content)));
        for (Map.Entry<String, String> entry : resources.entrySet()) {
            update(digest, "resource");
            update(digest, entry.getKey());
            update(digest, entry.getValue());
        }
        return hex(digest.digest());
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
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        value.forEach(
                (key, item) -> {
                    String normalizedKey = requireText(key, name + " key");
                    if (normalizedKey.length() > MAX_KEY_LENGTH) {
                        throw new IllegalArgumentException(name + " key is too long");
                    }
                    rejectPlatformKey(normalizedKey);
                    result.put(normalizedKey, immutableJsonValue(item, name + "." + normalizedKey));
                });
        if (requireSchemaVersion && !result.containsKey("schemaVersion")) {
            throw new IllegalArgumentException(name + " must contain schemaVersion");
        }
        return Collections.unmodifiableMap(result);
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

    private static Object immutableJsonValue(Object value, String name) {
        if (value == null || value instanceof String || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Number number) {
            requireFinite(number, name);
            return number;
        }
        if (value instanceof Map<?, ?> map) {
            LinkedHashMap<String, Object> copy = new LinkedHashMap<>();
            map.forEach(
                    (key, item) -> {
                        if (!(key instanceof String stringKey)) {
                            throw new IllegalArgumentException(name + " keys must be strings");
                        }
                        String normalizedKey = requireText(stringKey, name + " key");
                        rejectPlatformKey(normalizedKey);
                        copy.put(
                                normalizedKey,
                                immutableJsonValue(item, name + "." + normalizedKey));
                    });
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (int i = 0; i < list.size(); i++) {
                copy.add(immutableJsonValue(list.get(i), name + "[" + i + "]"));
            }
            return List.copyOf(copy);
        }
        throw new IllegalArgumentException(name + " is not JSON-compatible");
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

    private static String normalizePath(String value) {
        String path = requireText(value, "resource path").replace('\\', '/');
        while (path.startsWith("./")) {
            path = path.substring(2);
        }
        if (path.startsWith("/")
                || path.equals("..")
                || path.startsWith("../")
                || path.contains("/../")) {
            throw new IllegalArgumentException("resource path must be relative and normalized");
        }
        return path;
    }

    private static String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\r\n", "\n").replace('\r', '\n');
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
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
