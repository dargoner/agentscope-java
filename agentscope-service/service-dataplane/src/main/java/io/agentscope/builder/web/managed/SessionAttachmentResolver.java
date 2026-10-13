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
package io.agentscope.builder.web.managed;

import io.agentscope.core.session.JournalSessionLog;
import io.agentscope.harness.agent.attachment.AttachmentMetadata;
import io.agentscope.harness.agent.attachment.AttachmentReference;
import io.agentscope.harness.agent.attachment.AttachmentResolveContext;
import io.agentscope.harness.agent.attachment.AttachmentResolver;
import io.agentscope.harness.agent.attachment.AttachmentStore;
import io.agentscope.harness.agent.attachment.AttachmentStoreCategory;
import io.agentscope.harness.agent.attachment.AttachmentStoreException;
import io.agentscope.harness.agent.attachment.AttachmentStoreScope;
import io.agentscope.harness.agent.attachment.ResolvedAttachment;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Adapts a session/user-bound {@link AttachmentStore} to the harness {@link AttachmentResolver}.
 *
 * <p>An instance is bound to one {@code (userId, sessionId)} pair captured at agent build time and
 * refuses any resolve context that does not match it, so an agent cannot read another scope's
 * attachments. Blocking store reads run on {@code boundedElastic} under a fixed finite timeout — a
 * stalled store degrades to {@link ResolvedAttachment.Unavailable.Category#TIMEOUT} — cancellation
 * propagates and no internal URL or raw exception message is ever returned.
 */
public final class SessionAttachmentResolver implements AttachmentResolver {

    private static final Duration RESOLVE_TIMEOUT = Duration.ofSeconds(10);

    private final AttachmentStore store;
    private final AttachmentStoreScope scope;
    private final Duration resolveTimeout;

    public SessionAttachmentResolver(AttachmentStore store, String userId, String sessionId) {
        this(store, userId, sessionId, RESOLVE_TIMEOUT);
    }

    SessionAttachmentResolver(
            AttachmentStore store, String userId, String sessionId, Duration resolveTimeout) {
        this.store = Objects.requireNonNull(store, "store");
        this.scope = new AttachmentStoreScope(userId, sessionId);
        this.resolveTimeout = Objects.requireNonNull(resolveTimeout, "resolveTimeout");
    }

    @Override
    public Mono<ResolvedAttachment> resolve(
            AttachmentReference reference, AttachmentResolveContext context) {
        if (context == null
                || !scope.userId().equals(context.userId())
                || !scope.sessionId().equals(context.sessionId()))
            return Mono.just(
                    new ResolvedAttachment.Unavailable(
                            ResolvedAttachment.Unavailable.Category.FORBIDDEN, "scope"));
        return Mono.fromCallable(() -> resolveBlocking(reference))
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(
                        resolveTimeout,
                        Mono.just(
                                new ResolvedAttachment.Unavailable(
                                        ResolvedAttachment.Unavailable.Category.TIMEOUT,
                                        "timeout")));
    }

    private ResolvedAttachment resolveBlocking(AttachmentReference reference) {
        try {
            AttachmentMetadata metadata = store.resolve(scope, reference.referenceId());
            byte[] bytes;
            try (InputStream in = store.open(scope, reference.referenceId())) {
                bytes = in.readAllBytes();
            }
            if (!consistent(reference, metadata, bytes))
                return new ResolvedAttachment.Unavailable(
                        ResolvedAttachment.Unavailable.Category.FAILED, "mismatch");
            if (bytes.length == 0)
                return new ResolvedAttachment.Unavailable(
                        ResolvedAttachment.Unavailable.Category.UNSUPPORTED, "empty");
            if (metadata.mediaType() == null || metadata.mediaType().isBlank())
                return new ResolvedAttachment.Unavailable(
                        ResolvedAttachment.Unavailable.Category.MIME_MISMATCH, "media");
            if (reference.mediaType() != null
                    && !reference.mediaType().equals(metadata.mediaType()))
                return new ResolvedAttachment.Unavailable(
                        ResolvedAttachment.Unavailable.Category.MIME_MISMATCH, "media");
            return new ResolvedAttachment.Inline(
                    metadata.mediaType(), Base64.getEncoder().encodeToString(bytes));
        } catch (AttachmentStoreException error) {
            return new ResolvedAttachment.Unavailable(category(error.category()), "store");
        } catch (IOException error) {
            return new ResolvedAttachment.Unavailable(
                    ResolvedAttachment.Unavailable.Category.FAILED, "read");
        } catch (CancellationException error) {
            throw error;
        } catch (RuntimeException error) {
            return new ResolvedAttachment.Unavailable(
                    ResolvedAttachment.Unavailable.Category.FAILED, "resolver");
        }
    }

    // Guards against a store record substituted between registration and replay: the historical
    // reference and the freshly read bytes must agree on content hash and size. A media-type
    // drift is classified separately as MIME_MISMATCH.
    private static boolean consistent(
            AttachmentReference reference, AttachmentMetadata metadata, byte[] bytes) {
        if (metadata.sha256() != null && !metadata.sha256().equals(JournalSessionLog.hash(bytes)))
            return false;
        if (reference.sha256() != null && !reference.sha256().equals(metadata.sha256()))
            return false;
        return metadata.size() == bytes.length && reference.size() == bytes.length;
    }

    private static ResolvedAttachment.Unavailable.Category category(
            AttachmentStoreCategory category) {
        return switch (category) {
            case NOT_FOUND -> ResolvedAttachment.Unavailable.Category.NOT_FOUND;
            case FORBIDDEN -> ResolvedAttachment.Unavailable.Category.FORBIDDEN;
            case TOO_LARGE -> ResolvedAttachment.Unavailable.Category.TOO_LARGE;
            default -> ResolvedAttachment.Unavailable.Category.FAILED;
        };
    }
}
