/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.spring.boot.agui.common;

import io.agentscope.core.state.AgentStateStore;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;

/** Resolves the explicitly opted-in, unique store for distributed AG-UI resume coordination. */
public final class AguiResumeStateStoreResolver {

    private AguiResumeStateStoreResolver() {}

    public static AgentStateStore resolve(
            AguiProperties properties, ObjectProvider<AgentStateStore> stores) {
        if (!properties.isResumeDistributedEnabled()) {
            return null;
        }
        List<AgentStateStore> candidates = stores.orderedStream().toList();
        if (candidates.size() != 1) {
            throw new IllegalStateException(
                    "agentscope.agui.resume.distributed-enabled requires exactly one"
                            + " AgentStateStore bean");
        }
        AgentStateStore store = candidates.get(0);
        if (!store.supportsVersioning()) {
            throw new IllegalStateException(
                    "The AgentStateStore bean used for AG-UI resume coordination must support"
                            + " versioning");
        }
        return store;
    }
}
