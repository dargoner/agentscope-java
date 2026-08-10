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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.agentscope.core.agui.registry.AguiAgentRegistry;
import io.agentscope.core.state.InMemoryAgentStateStore;
import io.agentscope.spring.boot.agui.mvc.AguiMvcController;
import io.agentscope.spring.boot.agui.webflux.AguiWebFluxHandler;
import org.junit.jupiter.api.Test;

class AguiTransportBuilderTest {

    @Test
    void mvcBuilderAcceptsResumeStateStore() {
        assertDoesNotThrow(
                () ->
                        AguiMvcController.builder()
                                .agentRegistry(new AguiAgentRegistry())
                                .resumeStateStore(new InMemoryAgentStateStore())
                                .build());
    }

    @Test
    void webFluxBuilderAcceptsResumeStateStore() {
        assertDoesNotThrow(
                () ->
                        AguiWebFluxHandler.builder()
                                .agentRegistry(new AguiAgentRegistry())
                                .resumeStateStore(new InMemoryAgentStateStore())
                                .build());
    }
}
