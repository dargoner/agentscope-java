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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.util.MarkdownSkillParser;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SkillEvolutionContractTest {

    @Test
    void payloadIsDeeplyImmutableAndVersioned() {
        List<Object> nested = new ArrayList<>();
        nested.add("initial");
        nested.add(null);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("allowed", nested);

        SkillEvolutionPayload payload = new SkillEvolutionPayload("test.payload", 1, data);
        nested.add("changed");

        assertEquals(java.util.Arrays.asList("initial", null), payload.data().get("allowed"));
        assertThrows(UnsupportedOperationException.class, () -> payload.data().put("new", true));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("Invalid Schema", 1, Map.of()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.payload", 0, Map.of()));
    }

    @Test
    void payloadSnapshotsMutableNumbersAndRejectsPaddedKeys() {
        AtomicInteger mutableNumber = new AtomicInteger(7);
        SkillEvolutionPayload payload =
                new SkillEvolutionPayload(
                        "test.payload", 1, Map.of("nested", Map.of("count", mutableNumber)));

        mutableNumber.set(9);

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) payload.data().get("nested");
        assertEquals(new BigDecimal("7"), nested.get("count"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.payload", 1, Map.of(" padded ", true)));
    }

    @Test
    void payloadRejectsCyclicAndExcessivelyDeepContainers() {
        Map<String, Object> cyclic = new LinkedHashMap<>();
        cyclic.put("self", cyclic);
        List<Object> cyclicList = new ArrayList<>();
        cyclicList.add(cyclicList);

        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.payload", 1, cyclic));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.payload", 1, Map.of("self", cyclicList)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.payload", 1, excessivelyNestedMetadata()));
    }

    @Test
    void canonicalHashIsStableAcrossLineEndingsAndResourceOrder() {
        AgentSkill first =
                new AgentSkill(
                        "demo",
                        "demo skill",
                        "first\r\nsecond\r\n",
                        linkedResources("b.txt", "two\r\n", "a.txt", "one\n"));
        AgentSkill second =
                new AgentSkill(
                        "demo",
                        "demo skill",
                        "first\nsecond\n",
                        linkedResources("a.txt", "one\n", "b.txt", "two\n"));

        SkillCandidateArtifact firstArtifact =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), first, 0);
        SkillCandidateArtifact secondArtifact =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), second, 0);

        assertEquals(firstArtifact.contentHash(), secondArtifact.contentHash());
        assertTrue(firstArtifact.verifyIntegrity());
    }

    @Test
    void materializerIsTheCanonicalArtifactBoundary() {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("name", "demo");
        metadata.put("description", "line one\r\nline two");
        metadata.put("settings", linkedMetadata("z", 1.00d, "a", "first\rline"));
        AgentSkill skill =
                new AgentSkill(
                        metadata,
                        "# Instructions\r\nRun safely.\r\n",
                        linkedResources("z.txt", "last\r\n", "./docs//guide.txt", "guide\rbody"),
                        null);

        Map<String, byte[]> files = SkillArtifactMaterializer.materialize(skill);
        String markdown =
                new String(files.get(SkillArtifactMaterializer.SKILL_FILE), StandardCharsets.UTF_8);
        MarkdownSkillParser.ParsedMarkdown parsed = MarkdownSkillParser.parse(markdown);

        assertEquals(List.of("SKILL.md", "docs/guide.txt", "z.txt"), List.copyOf(files.keySet()));
        assertTrue(markdown.startsWith("---\n"));
        assertEquals("demo", parsed.getMetadata().get("name"));
        assertEquals("line one\nline two", parsed.getMetadata().get("description"));
        assertEquals("# Instructions\nRun safely.\n", parsed.getContent());
        assertEquals(
                "guide\nbody", new String(files.get("docs/guide.txt"), StandardCharsets.UTF_8));
        assertEquals(CanonicalSkillHasher.hashMaterialized(files), SkillArtifactHasher.hash(skill));
    }

    @Test
    void materializedBytesAreStableAcrossMetadataAndResourceOrderAndNumberForms() {
        Map<String, Object> firstMetadata = new LinkedHashMap<>();
        firstMetadata.put("name", "demo");
        firstMetadata.put("description", "demo\r\nskill");
        firstMetadata.put("settings", linkedMetadata("z", 1, "a", List.of("x\r\ny")));
        Map<String, Object> secondMetadata = new LinkedHashMap<>();
        secondMetadata.put("settings", linkedMetadata("a", List.of("x\ny"), "z", BigInteger.ONE));
        secondMetadata.put("description", "demo\nskill");
        secondMetadata.put("name", "demo");
        AgentSkill first =
                new AgentSkill(
                        firstMetadata,
                        "body\r\n",
                        linkedResources("b.txt", "two\r\n", "a.txt", "one"),
                        null);
        AgentSkill second =
                new AgentSkill(
                        secondMetadata,
                        "body\n",
                        linkedResources("a.txt", "one", "b.txt", "two\n"),
                        null);

        Map<String, byte[]> firstFiles = SkillArtifactMaterializer.materialize(first);
        Map<String, byte[]> secondFiles = SkillArtifactMaterializer.materialize(second);

        assertEquals(firstFiles.keySet(), secondFiles.keySet());
        firstFiles.forEach(
                (path, content) -> assertArrayEquals(content, secondFiles.get(path), path));
        assertEquals(SkillArtifactHasher.hash(first), SkillArtifactHasher.hash(second));
    }

    @Test
    void materializerPreservesYamlNumberAndDateSemanticsWithDefensiveSnapshots() {
        MarkdownSkillParser.ParsedMarkdown parsed =
                MarkdownSkillParser.parse(
                        "---\n"
                                + "name: typed\n"
                                + "description: typed metadata\n"
                                + "count: 7\n"
                                + "ratio: 7.00\n"
                                + "releasedAt: 2026-08-28T12:34:56Z\n"
                                + "---\n"
                                + "body");
        Date releasedAt = (Date) parsed.getMetadata().get("releasedAt");
        AgentSkill skill =
                new AgentSkill(parsed.getMetadata(), parsed.getContent(), Map.of(), null);
        SkillCandidateArtifact artifact =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), skill, 0);
        String markdown =
                new String(
                        SkillArtifactMaterializer.materialize(artifact.candidate())
                                .get(SkillArtifactMaterializer.SKILL_FILE),
                        StandardCharsets.UTF_8);
        Map<String, Object> reparsed = MarkdownSkillParser.parse(markdown).getMetadata();

        assertTrue(isIntegralNumber(reparsed.get("count")));
        assertTrue(isDecimalNumber(reparsed.get("ratio")));
        assertEquals(releasedAt, reparsed.get("releasedAt"));

        releasedAt.setTime(0);
        Date snapshot = (Date) artifact.candidate().getMetadataValue("releasedAt");
        assertEquals(Instant.parse("2026-08-28T12:34:56Z"), snapshot.toInstant());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.setTime(0));
        assertTrue(artifact.verifyIntegrity());
    }

    @Test
    void materializerRejectsCyclicAndExcessivelyDeepMetadata() {
        Map<String, Object> cyclic = new LinkedHashMap<>();
        cyclic.put("self", cyclic);
        List<Object> cyclicList = new ArrayList<>();
        cyclicList.add(cyclicList);

        assertThrows(
                IllegalArgumentException.class,
                () -> SkillArtifactMaterializer.canonicalMetadata(cyclic));
        assertThrows(
                IllegalArgumentException.class,
                () -> SkillArtifactMaterializer.canonicalMetadata(Map.of("self", cyclicList)));
        assertThrows(
                IllegalArgumentException.class,
                () -> SkillArtifactMaterializer.canonicalMetadata(excessivelyNestedMetadata()));
    }

    @Test
    void completeMaterializedArtifactAndHashMatchGoldenVector() {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("settings", linkedMetadata("ratio", new BigDecimal("1.50"), "count", 2));
        metadata.put("name", "golden");
        metadata.put("description", "Golden skill");
        AgentSkill skill =
                new AgentSkill(
                        metadata,
                        "# Instructions\r\nRun safely.\r\n",
                        linkedResources("notes/b.txt", "two\r\n", "notes/a.txt", "one"),
                        null);
        Map<String, byte[]> files = SkillArtifactMaterializer.materialize(skill);

        assertEquals(
                List.of("SKILL.md", "notes/a.txt", "notes/b.txt"), List.copyOf(files.keySet()));
        assertArrayEquals(
                ("---\n"
                                + "description: Golden skill\n"
                                + "name: golden\n"
                                + "settings:\n"
                                + "  count: 2\n"
                                + "  ratio: 1.5\n"
                                + "---\n\n"
                                + "# Instructions\n"
                                + "Run safely.\n")
                        .getBytes(StandardCharsets.UTF_8),
                files.get("SKILL.md"));
        assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), files.get("notes/a.txt"));
        assertArrayEquals("two\n".getBytes(StandardCharsets.UTF_8), files.get("notes/b.txt"));
        assertEquals(
                "9894b6e7f307eeae88774725ee60f1671df196fb849825122f871d51379a688b",
                SkillArtifactHasher.hash(skill));
    }

    @Test
    void materializerRejectsSkillFileAliasesAndFileDirectoryConflicts() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillArtifactMaterializer.materialize(
                                new AgentSkill(
                                        "demo",
                                        "demo skill",
                                        "body",
                                        Map.of("./SKILL.md", "fake"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillArtifactMaterializer.materialize(
                                new AgentSkill(
                                        "demo", "demo skill", "body", Map.of("skill.md", "fake"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillArtifactMaterializer.materialize(
                                new AgentSkill(
                                        "demo",
                                        "demo skill",
                                        "body",
                                        linkedResources(
                                                "docs", "file", "docs/readme.md", "nested"))));
    }

    @Test
    void materializerRejectsControlCharactersAndPortablePathAliases() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillArtifactMaterializer.materialize(
                                new AgentSkill(
                                        "demo",
                                        "demo skill",
                                        "body",
                                        Map.of("bad\u0000.txt", "x"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillArtifactMaterializer.materialize(
                                new AgentSkill(
                                        "demo",
                                        "demo skill",
                                        "body",
                                        linkedResources(
                                                "Docs/e\u0301.txt",
                                                "first",
                                                "docs/\u00c9.TXT",
                                                "second"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillArtifactMaterializer.materialize(
                                new AgentSkill(
                                        "demo",
                                        "demo skill",
                                        "body",
                                        linkedResources(
                                                "Docs", "file", "docs/readme.md", "nested"))));
    }

    @Test
    void canonicalHashIncludesAllSemanticMetadata() {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("name", "demo");
        metadata.put("description", "demo skill");
        metadata.put("homepage", "https://example.test/skill");
        metadata.put("metadata", Map.of("tags", List.of("safe", "reviewed")));
        AgentSkill original = new AgentSkill(metadata, "content", Map.of(), null);
        Map<String, Object> reorderedMetadata = new LinkedHashMap<>();
        reorderedMetadata.put("metadata", Map.of("tags", List.of("safe", "reviewed")));
        reorderedMetadata.put("homepage", "https://example.test/skill");
        reorderedMetadata.put("description", "demo skill");
        reorderedMetadata.put("name", "demo");
        AgentSkill reordered = new AgentSkill(reorderedMetadata, "content", Map.of(), null);

        assertEquals(SkillArtifactHasher.hash(original), SkillArtifactHasher.hash(reordered));
        assertNotEquals(
                SkillArtifactHasher.hash(original),
                SkillArtifactHasher.hash(original.toBuilder().name("renamed-demo").build()));
        assertNotEquals(
                SkillArtifactHasher.hash(original),
                SkillArtifactHasher.hash(
                        original.toBuilder().description("changed description").build()));
        assertNotEquals(
                SkillArtifactHasher.hash(original),
                SkillArtifactHasher.hash(
                        original.toBuilder()
                                .putMetadata("homepage", "https://example.test/tampered")
                                .build()));
        assertNotEquals(
                SkillArtifactHasher.hash(original),
                SkillArtifactHasher.hash(
                        original.toBuilder()
                                .putMetadata(
                                        "metadata", Map.of("tags", List.of("safe", "tampered")))
                                .build()));
    }

    @Test
    void canonicalHashRejectsSlashNormalizationCollisions() {
        AgentSkill skill =
                new AgentSkill(
                        "demo",
                        "demo skill",
                        "content",
                        linkedResources(
                                "scripts/run.sh", "expected", "scripts\\run.sh", "tampered"));

        assertThrows(IllegalArgumentException.class, () -> SkillArtifactHasher.hash(skill));
    }

    @Test
    void duplicateCanonicalResourcePathCannotHideTamperedContent() {
        AgentSkill skill =
                new AgentSkill(
                        "demo",
                        "demo skill",
                        "content",
                        linkedResources(
                                "references/result.txt",
                                "expected",
                                "./references/result.txt",
                                "tampered"));

        assertThrows(IllegalArgumentException.class, () -> SkillArtifactHasher.hash(skill));
    }

    @Test
    void publicConstructorRejectsTamperedContentHash() {
        AgentSkill skill = skill("original");
        String validHash =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), skill, 0)
                        .contentHash();
        AgentSkill changed = skill("changed");

        assertNotEquals(
                validHash,
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), changed, 0)
                        .contentHash());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillCandidateArtifact(
                                SkillEvolutionType.CREATE,
                                List.of(),
                                changed,
                                SkillArtifactHasher.ALGORITHM,
                                validHash,
                                0));
    }

    @Test
    void canonicalHashRejectsResourcesOutsideCandidateRoot() {
        AgentSkill skill =
                new AgentSkill("demo", "demo skill", "content", Map.of("../escape.txt", "blocked"));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillCandidateArtifact.create(
                                SkillEvolutionType.CREATE, List.of(), skill, 0));
    }

    @Test
    void sourceCardinalityAndPatchIterationAreEnforced() {
        AgentSkill sourceSkill = skill("source");
        SkillRevisionRef source = revision("source", sourceSkill);
        SkillEvolutionPayload constraints = payload("constraints");

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillCandidateGenerationRequest(
                                SkillEvolutionType.REFINE,
                                List.of(),
                                List.of(),
                                Optional.empty(),
                                List.of(),
                                Optional.empty(),
                                0,
                                constraints));

        SkillCandidateArtifact previous =
                SkillCandidateArtifact.create(
                        SkillEvolutionType.REFINE, List.of(source), sourceSkill, 0);
        SkillCandidateGenerationRequest patch =
                new SkillCandidateGenerationRequest(
                        SkillEvolutionType.REFINE,
                        List.of(source),
                        List.of(sourceSkill),
                        Optional.of(previous),
                        List.of(payload("evidence")),
                        Optional.of(payload("feedback")),
                        1,
                        constraints);

        assertEquals(1, patch.iteration());
        assertEquals(previous, patch.previousCandidate().orElseThrow());
    }

    @Test
    void mergeRejectsDuplicateSourceReferences() {
        AgentSkill sourceSkill = skill("source");
        SkillRevisionRef source = revision("source", sourceSkill);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillCandidateGenerationRequest(
                                SkillEvolutionType.MERGE,
                                List.of(source, source),
                                List.of(sourceSkill, sourceSkill),
                                Optional.empty(),
                                List.of(),
                                Optional.empty(),
                                0,
                                payload("constraints")));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SkillCandidateArtifact.create(
                                SkillEvolutionType.MERGE,
                                List.of(source, source),
                                skill("candidate"),
                                0));
    }

    @Test
    void generationRequestBindsEachDeclaredSourceToItsActualArtifact() {
        AgentSkill declared = skill("declared");
        AgentSkill substituted = skill("substituted");
        SkillRevisionRef source = revision("source", declared);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillCandidateGenerationRequest(
                                SkillEvolutionType.REFINE,
                                List.of(source),
                                List.of(substituted),
                                Optional.empty(),
                                List.of(),
                                Optional.empty(),
                                0,
                                payload("constraints")));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillRevisionRef(
                                "source",
                                "1",
                                "unsupported-hash",
                                SkillArtifactHasher.hash(declared)));
    }

    @Test
    void patchRequestRejectsPreviousCandidateFromDifferentSources() {
        AgentSkill sourceSkill = skill("source");
        SkillRevisionRef previousSource = revision("source-a", sourceSkill);
        SkillRevisionRef currentSource = revision("source-b", sourceSkill);
        SkillCandidateArtifact previous =
                SkillCandidateArtifact.create(
                        SkillEvolutionType.REFINE,
                        List.of(previousSource),
                        skill("previous candidate"),
                        0);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillCandidateGenerationRequest(
                                SkillEvolutionType.REFINE,
                                List.of(currentSource),
                                List.of(sourceSkill),
                                Optional.of(previous),
                                List.of(),
                                Optional.empty(),
                                1,
                                payload("constraints")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void generationRequestSnapshotsMutableSourceMetadata() {
        List<Object> tags = new ArrayList<>(List.of("reviewed"));
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("name", "source");
        metadata.put("description", "source skill");
        metadata.put("metadata", Map.of("tags", tags));
        AgentSkill sourceSkill = new AgentSkill(metadata, "source", Map.of(), null);
        SkillRevisionRef source = revision("source", sourceSkill);
        SkillCandidateGenerationRequest request =
                new SkillCandidateGenerationRequest(
                        SkillEvolutionType.REFINE,
                        List.of(source),
                        List.of(sourceSkill),
                        Optional.empty(),
                        List.of(),
                        Optional.empty(),
                        0,
                        payload("constraints"));

        tags.add("tampered");

        AgentSkill snapshot = request.sourceSkills().get(0);
        Map<String, Object> nested = (Map<String, Object>) snapshot.getMetadataValue("metadata");
        assertEquals(List.of("reviewed"), nested.get("tags"));
        assertTrue(
                SkillArtifactHasher.verify(
                        snapshot, source.contentHashAlgorithm(), source.contentHash()));
        assertThrows(
                UnsupportedOperationException.class,
                () -> ((List<Object>) nested.get("tags")).add("mutation"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void candidateArtifactSnapshotsMutableCandidateMetadata() {
        List<Object> tags = new ArrayList<>(List.of("approved"));
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("name", "candidate");
        metadata.put("description", "candidate skill");
        metadata.put("metadata", Map.of("tags", tags));
        AgentSkill candidate = new AgentSkill(metadata, "candidate", Map.of(), null);

        SkillCandidateArtifact artifact =
                SkillCandidateArtifact.create(SkillEvolutionType.CREATE, List.of(), candidate, 0);
        tags.add("tampered");

        Map<String, Object> nested =
                (Map<String, Object>) artifact.candidate().getMetadataValue("metadata");
        assertEquals(List.of("approved"), nested.get("tags"));
        assertTrue(artifact.verifyIntegrity());
    }

    @Test
    void requestPayloadsRejectPlatformFields() {
        List<Object> nested = new ArrayList<>();
        nested.add("initial");
        Map<String, Object> constraintData = new LinkedHashMap<>();
        constraintData.put("allowed", nested);
        SkillCandidateGenerationRequest request =
                new SkillCandidateGenerationRequest(
                        SkillEvolutionType.CREATE,
                        List.of(),
                        List.of(),
                        Optional.empty(),
                        List.of(),
                        Optional.empty(),
                        0,
                        new SkillEvolutionPayload("test.constraints", 1, constraintData));
        nested.add("changed");

        assertEquals(List.of("initial"), request.constraints().data().get("allowed"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> request.constraints().data().put("new", true));

        Map<String, Object> invalid = new LinkedHashMap<>();
        invalid.put("workspace_id", "forbidden");
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.constraints", 1, invalid));
    }

    @Test
    void finalGateRejectsDisclosedFeedback() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillValidationReport(
                                SkillValidationStage.FINAL_GATE,
                                false,
                                "a".repeat(64),
                                metrics(0.4),
                                Optional.of(payload("feedback"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillValidationReport(
                                SkillValidationStage.DEVELOPMENT,
                                false,
                                "b".repeat(64),
                                new SkillEvolutionPayload(
                                        "test.metrics", 1, Map.of("score", "not-a-number")),
                                Optional.empty()));
    }

    @Test
    void validationRequestRequiresPositiveTimeoutAndSchemaVersion() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SkillValidationRequest(
                                SkillValidationStage.DEVELOPMENT,
                                payload("validation-spec"),
                                payload("validation-constraints"),
                                Duration.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SkillEvolutionPayload("test.payload", 1, Map.of("score", Double.NaN)));
    }

    private static AgentSkill skill(String content) {
        return new AgentSkill("demo", "demo skill", content, Map.of());
    }

    private static SkillRevisionRef revision(String skillId, AgentSkill skill) {
        return new SkillRevisionRef(
                skillId, "1", SkillArtifactHasher.ALGORITHM, SkillArtifactHasher.hash(skill));
    }

    private static SkillEvolutionPayload payload(String name) {
        return SkillEvolutionPayload.versionOne("test." + name, Map.of());
    }

    private static SkillEvolutionPayload metrics(double score) {
        return SkillEvolutionPayload.versionOne("test.metrics", Map.of("score", score));
    }

    private static Map<String, Object> excessivelyNestedMetadata() {
        Map<String, Object> root = new LinkedHashMap<>();
        Map<String, Object> current = root;
        for (int i = 0; i < 66; i++) {
            Map<String, Object> child = new LinkedHashMap<>();
            current.put("level", child);
            current = child;
        }
        return root;
    }

    private static boolean isIntegralNumber(Object value) {
        return value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof BigInteger;
    }

    private static boolean isDecimalNumber(Object value) {
        return value instanceof Float || value instanceof Double || value instanceof BigDecimal;
    }

    private static Map<String, String> linkedResources(String... values) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put(values[i], values[i + 1]);
        }
        return result;
    }

    private static Map<String, Object> linkedMetadata(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put((String) values[i], values[i + 1]);
        }
        return result;
    }
}
