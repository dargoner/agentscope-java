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

import { describe, expect, it } from 'vitest';
import type { SessionEvent } from '../api/managedSessions';
import {
  applyChatFrame,
  chatDisplayBlocks,
  createChatDisplayState,
} from './ChatPanel';

function frame(
  id: string,
  type: string,
  payload?: Record<string, unknown>,
  attributes?: Record<string, unknown>,
): SessionEvent {
  return {
    id,
    sessionId: 'session-1',
    seq: -1,
    type,
    payload,
    attributes,
    createdAt: 1,
  };
}

function startFrame(eventId: string, type = 'agent.message'): SessionEvent {
  return frame(`start-${eventId}`, 'event_start', { event_id: eventId, type });
}

function deltaFrame(eventId: string, delta: string, type = 'agent.message'): SessionEvent {
  return frame(`delta-${eventId}`, 'event_delta', { event_id: eventId, type, delta });
}

function updateFrame(eventId: string, attributes: Record<string, unknown>): SessionEvent {
  return frame(
    `update-${eventId}`,
    'event_update',
    { event_id: eventId, type: 'agent.message' },
    attributes,
  );
}

function persistedAgentMessage(eventId: string, text: string): SessionEvent {
  return frame(eventId, 'agent.message', { text });
}

describe('chat display state', () => {
  it('moves intermediate preview to commentary and confirms only authoritative result', () => {
    let state = createChatDisplayState();
    const applyFrame = (event: SessionEvent) => {
      state = applyChatFrame(state, event);
    };

    applyFrame(startFrame('preview-1'));
    applyFrame(deltaFrame('preview-1', 'working'));
    applyFrame(updateFrame('preview-1', { disposition: 'INTERMEDIATE' }));

    expect(state.commentarySegments['preview-1'].text).toBe('working');
    expect(state.pendingSegments['preview-1']).toBeUndefined();
    expect(state.finalAnswer).toBeUndefined();

    applyFrame(startFrame('preview-2'));
    applyFrame(deltaFrame('preview-2', 'draft'));
    applyFrame(persistedAgentMessage('preview-2', 'answer'));

    expect(state.pendingSegments['preview-2']).toBeUndefined();
    expect(state.finalAnswer?.text).toBe('answer');
  });

  it('removes an empty authoritative preview without creating a placeholder answer', () => {
    let state = createChatDisplayState();
    state = applyChatFrame(state, startFrame('preview-empty'));
    state = applyChatFrame(state, deltaFrame('preview-empty', 'stale preview'));
    state = applyChatFrame(state, updateFrame('preview-empty', {
      authoritative: true,
      hasOutput: false,
    }));

    expect(state.pendingSegments['preview-empty']).toBeUndefined();
    expect(state.finalAnswer).toBeUndefined();
    expect(chatDisplayBlocks(state).map(block => block.text)).not.toContain('[agent response]');
  });

  it('keeps thinking and tool execution separate from answer text', () => {
    let state = createChatDisplayState();
    state = applyChatFrame(state, deltaFrame('thinking-1', 'checking', 'agent.thinking'));
    state = applyChatFrame(state, startFrame('tool-1', 'agent.tool_use'));
    state = applyChatFrame(state, deltaFrame('tool-1', '{"city":', 'agent.tool_use'));
    state = applyChatFrame(state, deltaFrame('tool-1', '"Hangzhou"}', 'agent.tool_use'));

    expect(state.thinkingSegments['thinking-1'].text).toBe('checking');
    expect(state.toolExecutions['tool-1'].text).toBe('{"city":"Hangzhou"}');
    expect(state.finalAnswer).toBeUndefined();
  });
});
