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
package io.agentscope.harness.agent.attachment;

import io.agentscope.core.message.AudioBlock;
import io.agentscope.core.message.Base64Source;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.DataBlock;
import io.agentscope.core.message.ImageBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.Source;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.URLSource;
import io.agentscope.core.message.VideoBlock;
import io.agentscope.core.middleware.ModelCallInput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Compiles persisted attachment references into core media blocks at the provider request boundary.
 *
 * <p>The canonical conversation history keeps only a text placeholder plus the fixed {@link
 * AttachmentReference#METADATA_KEY} metadata. This compiler produces a request-scoped {@link
 * ModelCallInput} in which each placeholder is replaced by the resolved {@link ImageBlock}/{@link
 * AudioBlock}/{@link VideoBlock}/{@link DataBlock} or, when resolution fails, by a short structured
 * error marker. Neither the returned input nor its metadata ever carries a file id, signature URL or
 * Base64 back into history, and an unresolved reference is never sent as silent placeholder text.
 *
 * <p>Core layer only: the compiler never imports provider SDKs or product types.
 */
public final class AttachmentRequestCompiler {
    private static final Logger log = LoggerFactory.getLogger(AttachmentRequestCompiler.class);

    private final AttachmentResolver resolver;

    public AttachmentRequestCompiler(AttachmentResolver resolver) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
    }

    /** Whether any message in the list carries an attachment reference key. */
    public static boolean hasReferences(List<Msg> messages) {
        if (messages == null) return false;
        for (Msg message : messages) {
            Map<String, Object> metadata = message.getMetadata();
            if (metadata != null && metadata.containsKey(AttachmentReference.METADATA_KEY))
                return true;
        }
        return false;
    }

    /** Returns a request-scoped input with every resolvable reference compiled into a media block. */
    public Mono<ModelCallInput> compile(ModelCallInput input, AttachmentResolveContext context) {
        if (input == null) return Mono.empty();
        Map<String, Mono<ResolvedAttachment>> cache = new ConcurrentHashMap<>();
        return Flux.fromIterable(input.messages())
                .concatMap(message -> compileMessage(message, context, cache))
                .collectList()
                .map(
                        messages ->
                                new ModelCallInput(
                                        messages, input.tools(), input.options(), input.model()));
    }

    private Mono<Msg> compileMessage(
            Msg message,
            AttachmentResolveContext context,
            Map<String, Mono<ResolvedAttachment>> cache) {
        if (message.getRole() == MsgRole.SYSTEM) return Mono.just(message);
        ParsedReferences parsed = referencesOf(message);
        if (parsed == null) return Mono.just(message);
        return Flux.fromIterable(parsed.references())
                .concatMap(reference -> resolveOne(reference, context, cache))
                .collectList()
                .map(compiled -> rebuild(message, compiled, parsed.malformed()));
    }

    private Mono<CompiledBlock> resolveOne(
            AttachmentReference reference,
            AttachmentResolveContext context,
            Map<String, Mono<ResolvedAttachment>> cache) {
        Mono<ResolvedAttachment> resolved =
                cache.computeIfAbsent(
                        reference.referenceId(),
                        id ->
                                Mono.defer(() -> resolver.resolve(reference, context))
                                        .switchIfEmpty(
                                                Mono.just(
                                                        new ResolvedAttachment.Unavailable(
                                                                ResolvedAttachment.Unavailable
                                                                        .Category.FAILED,
                                                                "empty")))
                                        .onErrorResume(
                                                error -> {
                                                    if (error instanceof CancellationException)
                                                        return Mono.error(error);
                                                    log.debug(
                                                            "Attachment resolution failed: {}",
                                                            error.getClass().getSimpleName());
                                                    return Mono.just(
                                                            new ResolvedAttachment.Unavailable(
                                                                    ResolvedAttachment.Unavailable
                                                                            .Category.FAILED,
                                                                    "resolver_error"));
                                                })
                                        .cache());
        return resolved.map(
                value -> new CompiledBlock(reference.blockOrdinal(), blockFor(reference, value)));
    }

    private static Msg rebuild(Msg message, List<CompiledBlock> compiled, int malformed) {
        List<ContentBlock> content = new ArrayList<>(message.getContent());
        Set<Integer> assigned = new HashSet<>();
        for (CompiledBlock block : compiled) {
            int ordinal = block.ordinal();
            if (ordinal >= 0 && ordinal < content.size() && assigned.add(ordinal))
                content.set(ordinal, block.block());
            else content.add(block.block());
        }
        for (int i = 0; i < malformed; i++) content.add(unavailableMarker(null));
        Map<String, Object> metadata = new LinkedHashMap<>();
        if (message.getMetadata() != null) metadata.putAll(message.getMetadata());
        metadata.remove(AttachmentReference.METADATA_KEY);
        return Msg.builder()
                .id(message.getId())
                .name(message.getName())
                .role(message.getRole())
                .content(content)
                .metadata(metadata)
                .timestamp(message.getTimestamp())
                .usage(message.getUsage())
                .build();
    }

    private static ContentBlock blockFor(
            AttachmentReference reference, ResolvedAttachment resolved) {
        if (resolved instanceof ResolvedAttachment.Remote remote)
            return mediaBlock(
                    reference,
                    mediaType(reference, remote.mediaType()),
                    new URLSource(remote.url(), remote.mediaType()));
        if (resolved instanceof ResolvedAttachment.Inline inline)
            return mediaBlock(
                    reference,
                    mediaType(reference, inline.mediaType()),
                    new Base64Source(inline.mediaType(), inline.base64Data()));
        return unavailableMarker(((ResolvedAttachment.Unavailable) resolved).category());
    }

    private static ContentBlock mediaBlock(
            AttachmentReference reference, String mediaType, Source source) {
        String type = mediaType == null ? "" : mediaType.toLowerCase(Locale.ROOT);
        if (type.startsWith("image/")) return ImageBlock.builder().source(source).build();
        if (type.startsWith("audio/")) return AudioBlock.builder().source(source).build();
        if (type.startsWith("video/")) return VideoBlock.builder().source(source).build();
        return DataBlock.builder()
                .id(reference.referenceId())
                .name(reference.name())
                .source(source)
                .build();
    }

    private static String mediaType(AttachmentReference reference, String resolved) {
        if (resolved != null && !resolved.isBlank()) return resolved;
        return reference.mediaType();
    }

    private static ContentBlock unavailableMarker(
            ResolvedAttachment.Unavailable.Category category) {
        return TextBlock.builder()
                .text("[attachment unavailable: " + (category == null ? "FAILED" : category) + "]")
                .build();
    }

    private static ParsedReferences referencesOf(Msg message) {
        Map<String, Object> metadata = message.getMetadata();
        if (metadata == null || !metadata.containsKey(AttachmentReference.METADATA_KEY))
            return null;
        Object raw = metadata.get(AttachmentReference.METADATA_KEY);
        List<AttachmentReference> references = new ArrayList<>();
        int malformed = 0;
        if (raw instanceof List<?> entries) {
            for (Object entry : entries) {
                if (entry instanceof Map<?, ?> map) {
                    try {
                        references.add(AttachmentReference.fromMetadata(stringKeyed(map)));
                    } catch (RuntimeException malformedEntry) {
                        log.debug("Skipping malformed attachment reference entry");
                        malformed++;
                    }
                } else {
                    malformed++;
                }
            }
        } else {
            malformed++;
        }
        return new ParsedReferences(references, malformed);
    }

    private static Map<String, Object> stringKeyed(Map<?, ?> map) {
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach(
                (key, value) -> {
                    if (key != null) result.put(key.toString(), value);
                });
        return result;
    }

    private record CompiledBlock(int ordinal, ContentBlock block) {}

    private record ParsedReferences(List<AttachmentReference> references, int malformed) {}
}
