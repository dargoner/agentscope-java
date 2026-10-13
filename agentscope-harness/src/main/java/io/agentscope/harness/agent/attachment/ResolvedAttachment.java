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

/**
 * A resolved attachment expressed only in terms the core model layer can already represent.
 *
 * <p>A resolver must never return a raw internal URL, an access token or a file id it cannot render;
 * it either produces a value the harness can compile into a core media block or a structured error.
 * The {@code detail} on {@link Unavailable} is a short category description only — never a raw
 * exception message, URL or token.
 */
public sealed interface ResolvedAttachment {

    /** A remote URL the provider adapter is explicitly allowed to fetch from the model. */
    record Remote(String url, String mediaType) implements ResolvedAttachment {
        public Remote {
            if (url == null || url.isBlank()) throw new IllegalArgumentException("url is required");
        }
    }

    /** Bounded inline Base64 produced at the last moment for this request. */
    record Inline(String mediaType, String base64Data) implements ResolvedAttachment {
        public Inline {
            if (mediaType == null || mediaType.isBlank())
                throw new IllegalArgumentException("mediaType is required");
            if (base64Data == null || base64Data.isBlank())
                throw new IllegalArgumentException("base64Data is required");
        }
    }

    /** The attachment cannot be attached; {@code category} is the actionable, non-sensitive reason. */
    record Unavailable(Category category, String detail) implements ResolvedAttachment {
        public Unavailable {
            if (category == null) category = Category.FAILED;
        }

        /** Coarse, non-sensitive failure classification safe to surface to callers and logs. */
        public enum Category {
            UNSUPPORTED,
            TOO_LARGE,
            MIME_MISMATCH,
            NOT_FOUND,
            FORBIDDEN,
            TIMEOUT,
            CANCELLED,
            FAILED
        }
    }
}
