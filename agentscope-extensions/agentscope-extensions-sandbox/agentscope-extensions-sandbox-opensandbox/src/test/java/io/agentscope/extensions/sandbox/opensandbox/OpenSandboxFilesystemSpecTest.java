/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package io.agentscope.extensions.sandbox.opensandbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class OpenSandboxFilesystemSpecTest {
    @Test
    void defaultsToWorkspaceRootAndCreatesOpenSandboxClient() {
        OpenSandboxFilesystemSpec spec = new OpenSandboxFilesystemSpec();

        assertEquals("/workspace", spec.workspaceSpec().getRoot());
        assertInstanceOf(OpenSandboxClient.class, spec.createClient());
    }

    @Test
    void fluentConfigurationReturnsSameSpec() {
        OpenSandboxFilesystemSpec spec = new OpenSandboxFilesystemSpec();

        assertSame(spec, spec.endpoint("http://localhost:8080"));
        assertSame(spec, spec.image("ubuntu:24.04"));
        assertSame(spec, spec.cpu("2"));
        assertSame(spec, spec.memory("4Gi"));
    }
}
