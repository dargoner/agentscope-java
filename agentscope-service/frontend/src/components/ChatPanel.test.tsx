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
import { renderToStaticMarkup } from 'react-dom/server';
import type { SessionEvent } from '../api/managedSessions';
import {
  applyAssistantFrameToHistory,
  applyChatFrame,
  chatDisplayBlocks,
  createChatDisplayState,
  restoreChatHistory,
} from './ChatPanel';
import MessageBlock from './MessageBlock';

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
    { event_id: eventId, type: 'agent.message', ...attributes },
  );
}

function persistedAgentMessage(eventId: string, text: string): SessionEvent {
  return frame(eventId, 'agent.message', { text });
}

function persistedToolUse(eventId: string, toolId: string, toolName: string): SessionEvent {
  return frame(eventId, 'agent.tool_use', {
    id: toolId,
    name: toolName,
    input: { city: 'Hangzhou' },
  });
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

  it('settles a terminal preview without treating it as the final answer', () => {
    let state = createChatDisplayState();
    state = applyChatFrame(state, startFrame('preview-terminal'));
    state = applyChatFrame(state, deltaFrame('preview-terminal', 'candidate'));
    state = applyChatFrame(state, updateFrame('preview-terminal', {
      disposition: 'TERMINAL',
      generateReason: 'MODEL_STOP',
    }));

    expect(state.pendingSegments['preview-terminal'].pending).toBe(false);
    expect(state.finalAnswer).toBeUndefined();
    expect(chatDisplayBlocks(state)).toEqual([
      expect.objectContaining({
        id: 'preview-terminal',
        presentation: 'preview',
        text: 'candidate',
      }),
    ]);
  });

  it('continues a restored tool turn in the same assistant message', () => {
    const history = restoreChatHistory([
      frame('user-1', 'user.message', { text: 'weather?' }),
      persistedToolUse('tool-event-1', 'tool-call-1', 'weather'),
    ]);

    expect(history.openMessageId).toBe('tool-event-1-turn');
    expect(history.messages).toHaveLength(2);

    const resumed = applyAssistantFrameToHistory(
      history,
      deltaFrame('answer-preview', 'It is sunny.'),
    );

    expect(resumed.messages).toHaveLength(2);
    expect(resumed.messages[1].id).toBe('tool-event-1-turn');
    expect(resumed.messages[1].displayState?.toolExecutions['tool-call-1'].toolName)
      .toBe('weather');
    expect(resumed.messages[1].displayState?.pendingSegments['answer-preview'].text)
      .toBe('It is sunny.');
  });

  it('preserves wire order and presentation across repeated reply segments and tools', () => {
    let state = createChatDisplayState();
    state = applyChatFrame(state, deltaFrame('reply-1', 'checking'));
    state = applyChatFrame(state, updateFrame('reply-1', { disposition: 'INTERMEDIATE' }));
    state = applyChatFrame(state, startFrame('tool-preview-1', 'agent.tool_use'));
    state = applyChatFrame(state, persistedToolUse('tool-preview-1', 'tool-call-1', 'lookup'));
    state = applyChatFrame(state, deltaFrame('reply-1', 'writing'));
    state = applyChatFrame(state, updateFrame('reply-1', { disposition: 'INTERMEDIATE' }));
    state = applyChatFrame(state, deltaFrame('reply-1', 'answer'));
    state = applyChatFrame(state, persistedAgentMessage('reply-1', 'final answer'));

    const blocks = chatDisplayBlocks(state);
    expect(blocks.map(block => [block.presentation ?? 'tool', block.text])).toEqual([
      ['commentary', 'checking'],
      ['tool', ''],
      ['commentary', 'writing'],
      ['final', 'final answer'],
    ]);
    expect(new Set(blocks.map(block => block.id)).size).toBe(blocks.length);

    const html = renderToStaticMarkup(
      <MessageBlock role="assistant" blocks={blocks} defaultOpen />,
    );
    expect(html).toContain('Commentary');
    expect(html).toContain('Final answer');
    expect(html.indexOf('checking')).toBeLessThan(html.indexOf('lookup'));
    expect(html.indexOf('lookup')).toBeLessThan(html.indexOf('writing'));
    expect(html.indexOf('writing')).toBeLessThan(html.indexOf('final answer'));
  });
});
