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
import { SessionEvent } from '../api/managedSessions';
import { ContentBlock } from './MessageBlock';

type Role = 'user' | 'assistant' | 'system' | 'error';
interface Message { id: string; role: Role; blocks: ContentBlock[]; displayState?: ChatDisplayState; pending?: boolean; closed?: boolean; }
export interface ChatTextSegment {
  id: string;
  sourceId: string;
  text: string;
  pending: boolean;
}

export interface ChatToolExecution {
  id: string;
  toolName: string;
  text?: string;
  result?: string;
}

export type ChatTextPresentation = 'commentary' | 'thinking' | 'pending' | 'preview' | 'final';

export interface ChatDisplayEntry {
  key: string;
  kind: 'text' | 'tool';
  refId: string;
  presentation?: ChatTextPresentation;
}

export interface ChatDisplayState {
  pendingSegments: Record<string, ChatTextSegment>;
  commentarySegments: Record<string, ChatTextSegment>;
  thinkingSegments: Record<string, ChatTextSegment>;
  toolExecutions: Record<string, ChatToolExecution>;
  finalAnswer?: ChatTextSegment;
  displayOrder: ChatDisplayEntry[];
  activePendingSegmentIds: Record<string, string>;
  activeCommentarySegmentIds: Record<string, string>;
  knownDispositions: Record<string, 'INTERMEDIATE' | 'TERMINAL'>;
  nextSequence: number;
}

export function createChatDisplayState(): ChatDisplayState {
  return {
    pendingSegments: {},
    commentarySegments: {},
    thinkingSegments: {},
    toolExecutions: {},
    displayOrder: [],
    activePendingSegmentIds: {},
    activeCommentarySegmentIds: {},
    knownDispositions: {},
    nextSequence: 1,
  };
}

function cloneDisplayState(state: ChatDisplayState): ChatDisplayState {
  return {
    ...state,
    pendingSegments: { ...state.pendingSegments },
    commentarySegments: { ...state.commentarySegments },
    thinkingSegments: { ...state.thinkingSegments },
    toolExecutions: { ...state.toolExecutions },
    displayOrder: [...state.displayOrder],
    activePendingSegmentIds: { ...state.activePendingSegmentIds },
    activeCommentarySegmentIds: { ...state.activeCommentarySegmentIds },
    knownDispositions: { ...state.knownDispositions },
  };
}

function uniqueDisplayKey(state: ChatDisplayState, preferred: string): string {
  if (!state.displayOrder.some(entry => entry.key === preferred)) return preferred;
  let key: string;
  do {
    key = `${preferred}:${state.nextSequence++}`;
  } while (state.displayOrder.some(entry => entry.key === key));
  return key;
}

function ensurePendingSegment(state: ChatDisplayState, sourceId: string): string {
  const activeId = state.activePendingSegmentIds[sourceId];
  if (activeId && state.pendingSegments[activeId]) return activeId;
  const segmentId = uniqueDisplayKey(state, sourceId);
  state.pendingSegments[segmentId] = {
    id: segmentId,
    sourceId,
    text: '',
    pending: true,
  };
  state.activePendingSegmentIds[sourceId] = segmentId;
  state.displayOrder.push({
    key: segmentId,
    kind: 'text',
    refId: segmentId,
    presentation: 'pending',
  });
  return segmentId;
}

function ensureCommentarySegment(state: ChatDisplayState, sourceId: string): string {
  const activeId = state.activeCommentarySegmentIds[sourceId];
  if (activeId && state.commentarySegments[activeId]) return activeId;
  const segmentId = uniqueDisplayKey(state, sourceId);
  state.commentarySegments[segmentId] = {
    id: segmentId,
    sourceId,
    text: '',
    pending: false,
  };
  state.activeCommentarySegmentIds[sourceId] = segmentId;
  state.displayOrder.push({
    key: segmentId,
    kind: 'text',
    refId: segmentId,
    presentation: 'commentary',
  });
  return segmentId;
}

function closeActiveCommentarySegments(state: ChatDisplayState): void {
  state.activeCommentarySegmentIds = {};
}

function ensureToolExecution(state: ChatDisplayState, eventId: string): ChatToolExecution {
  const existing = state.toolExecutions[eventId];
  if (existing) return existing;
  const execution = { id: eventId, toolName: 'tool', text: '' };
  state.toolExecutions[eventId] = execution;
  state.displayOrder.push({
    key: uniqueDisplayKey(state, eventId),
    kind: 'tool',
    refId: eventId,
  });
  return execution;
}

/** Applies one persisted event or stream-only preview frame to the current assistant turn. */
export function applyChatFrame(state: ChatDisplayState, event: SessionEvent): ChatDisplayState {
  const payload = event.payload ?? {};
  const eventId = String(payload.event_id ?? event.id ?? '');
  const targetType = String(payload.type ?? '');

  if (event.type === 'event_start') {
    if (!eventId) return state;
    if (targetType === 'agent.message') {
      const next = cloneDisplayState(state);
      if (next.knownDispositions[eventId] === 'INTERMEDIATE') {
        ensureCommentarySegment(next, eventId);
      } else {
        ensurePendingSegment(next, eventId);
      }
      return next;
    }
    if (targetType === 'agent.tool_use') {
      const next = cloneDisplayState(state);
      closeActiveCommentarySegments(next);
      ensureToolExecution(next, eventId);
      return next;
    }
    return state;
  }

  if (event.type === 'event_delta') {
    const delta = payload.delta != null ? String(payload.delta) : '';
    if (!eventId || !delta) return state;
    if (targetType === 'agent.message') {
      const next = cloneDisplayState(state);
      if (next.knownDispositions[eventId] === 'INTERMEDIATE') {
        const segmentId = ensureCommentarySegment(next, eventId);
        next.commentarySegments[segmentId] = {
          ...next.commentarySegments[segmentId],
          text: `${next.commentarySegments[segmentId].text}${delta}`,
        };
      } else {
        const segmentId = ensurePendingSegment(next, eventId);
        next.pendingSegments[segmentId] = {
          ...next.pendingSegments[segmentId],
          text: `${next.pendingSegments[segmentId].text}${delta}`,
          pending: true,
        };
      }
      return next;
    }
    if (targetType === 'agent.thinking') {
      const next = cloneDisplayState(state);
      const existing = next.thinkingSegments[eventId];
      if (!existing) {
        next.thinkingSegments[eventId] = {
          id: eventId,
          sourceId: eventId,
          text: delta,
          pending: true,
        };
        next.displayOrder.push({
          key: uniqueDisplayKey(next, eventId),
          kind: 'text',
          refId: eventId,
          presentation: 'thinking',
        });
      } else {
        next.thinkingSegments[eventId] = { ...existing, text: `${existing.text}${delta}` };
      }
      return next;
    }
    if (targetType === 'agent.tool_use') {
      const next = cloneDisplayState(state);
      closeActiveCommentarySegments(next);
      const current = ensureToolExecution(next, eventId);
      next.toolExecutions[eventId] = {
        ...current,
        text: `${current.text ?? ''}${delta}`,
      };
      return next;
    }
    return state;
  }

  if (event.type === 'event_update' && targetType === 'agent.message' && eventId) {
    const attributes = { ...payload, ...(event.attributes ?? {}) };
    if (attributes.disposition === 'INTERMEDIATE') {
      const segmentId = state.activePendingSegmentIds[eventId];
      const pending = segmentId ? state.pendingSegments[segmentId] : undefined;
      const next = cloneDisplayState(state);
      next.knownDispositions[eventId] = 'INTERMEDIATE';
      if (!pending) return next;
      delete next.pendingSegments[segmentId];
      delete next.activePendingSegmentIds[eventId];
      next.commentarySegments[segmentId] = { ...pending, pending: false };
      next.activeCommentarySegmentIds[eventId] = segmentId;
      next.displayOrder = next.displayOrder.map(entry => entry.refId === segmentId
        ? { ...entry, presentation: 'commentary' }
        : entry);
      return next;
    }
    if (attributes.disposition === 'TERMINAL') {
      const segmentId = state.activePendingSegmentIds[eventId];
      const pending = segmentId ? state.pendingSegments[segmentId] : undefined;
      const next = cloneDisplayState(state);
      next.knownDispositions[eventId] = 'TERMINAL';
      if (!pending) return next;
      next.pendingSegments[segmentId] = { ...pending, pending: false };
      delete next.activePendingSegmentIds[eventId];
      next.displayOrder = next.displayOrder.map(entry => entry.refId === segmentId
        ? { ...entry, presentation: 'preview' }
        : entry);
      return next;
    }
    if (attributes.authoritative === true && attributes.hasOutput === false) {
      const removedIds = Object.values(state.pendingSegments)
        .filter(segment => segment.sourceId === eventId)
        .map(segment => segment.id);
      if (removedIds.length === 0) return state;
      const next = cloneDisplayState(state);
      for (const id of removedIds) delete next.pendingSegments[id];
      delete next.activePendingSegmentIds[eventId];
      next.displayOrder = next.displayOrder.filter(entry => !removedIds.includes(entry.refId));
      return next;
    }
    return state;
  }

  if (event.type === 'agent.message') {
    const text = payloadText(event.payload);
    const next = cloneDisplayState(state);
    const removedEntries = next.displayOrder.filter(entry => next.pendingSegments[entry.refId]);
    const insertionIndex = removedEntries.length > 0
      ? next.displayOrder.findIndex(entry => entry.key === removedEntries[0].key)
      : next.displayOrder.length;
    const removedIds = Object.keys(next.pendingSegments);
    for (const id of removedIds) delete next.pendingSegments[id];
    next.activePendingSegmentIds = {};
    next.activeCommentarySegmentIds = {};
    next.knownDispositions = {};
    next.displayOrder = next.displayOrder.filter(entry =>
      !removedIds.includes(entry.refId) && entry.presentation !== 'final');
    next.finalAnswer = text
      ? { id: event.id, sourceId: event.id, text, pending: false }
      : undefined;
    if (next.finalAnswer) {
      const key = removedEntries[0]?.key ?? uniqueDisplayKey(next, `final-${event.id}`);
      next.displayOrder.splice(insertionIndex, 0, {
        key,
        kind: 'text',
        refId: event.id,
        presentation: 'final',
      });
    }
    return next;
  }

  if (event.type === 'agent.tool_use') {
    const toolId = String(payload.id ?? payload.toolCallId ?? payload.toolUseId ?? event.id);
    if (!toolId) return state;
    const next = cloneDisplayState(state);
    closeActiveCommentarySegments(next);
    const preview = next.toolExecutions[event.id];
    if (!preview) ensureToolExecution(next, event.id);
    const source = preview ?? next.toolExecutions[event.id];
    if (event.id !== toolId) delete next.toolExecutions[event.id];
    next.toolExecutions[toolId] = {
      id: toolId,
      toolName: String(payload.name ?? payload.toolName ?? source?.toolName ?? 'tool'),
      text: source?.text ?? (payload.input != null ? JSON.stringify(payload.input) : undefined),
      result: source?.result,
    };
    next.displayOrder = next.displayOrder.map(entry => entry.kind === 'tool' && entry.refId === event.id
      ? { ...entry, refId: toolId }
      : entry);
    return next;
  }

  if (event.type === 'agent.tool_result') {
    const toolId = String(payload.tool_use_id ?? payload.toolCallId ?? payload.id ?? '');
    const current = state.toolExecutions[toolId];
    if (!toolId || !current) return state;
    const result = payload.output != null ? String(payload.output) : payloadText(event.payload);
    return {
      ...state,
      toolExecutions: {
        ...state.toolExecutions,
        [toolId]: { ...current, result },
      },
    };
  }

  return state;
}

export function chatDisplayBlocks(state: ChatDisplayState): ContentBlock[] {
  return state.displayOrder.flatMap<ContentBlock>(entry => {
    if (entry.kind === 'tool') {
      const tool = state.toolExecutions[entry.refId];
      return tool ? [{
        kind: 'tool' as const,
        id: tool.id,
        renderKey: entry.key,
        toolName: tool.toolName,
        text: tool.text,
        result: tool.result,
      }] : [];
    }
    const segment = entry.presentation === 'commentary'
      ? state.commentarySegments[entry.refId]
      : entry.presentation === 'thinking'
        ? state.thinkingSegments[entry.refId]
        : entry.presentation === 'final'
          ? state.finalAnswer
          : state.pendingSegments[entry.refId];
    return segment ? [{
      kind: 'text' as const,
      id: entry.key,
      sourceId: segment.sourceId,
      text: segment.text,
      presentation: entry.presentation,
    }] : [];
  });
}

let counter = 0;
const nextId = () => `m${Date.now().toString(36)}-${counter++}`;

function payloadText(payload?: Record<string, unknown>): string {
  if (!payload) return '';
  const text = payload.text ?? payload.message ?? payload.content;
  return text != null ? String(text) : '';
}

function errorText(evt: SessionEvent): string {
  const payload = evt.payload as Record<string, unknown> | undefined;
  const raw = payload?.error;
  const err = (typeof raw === 'object' && raw != null ? raw : {}) as Record<string, unknown>;
  const code = err.code != null ? String(err.code) : '';
  const message = err.message != null ? String(err.message) : '';
  const label = code ? `[${code}]` : '[error]';
  return `${label} ${message || 'Session turn failed'}`.trim();
}

export interface ChatHistoryState {
  messages: Message[];
  openMessageId: string | null;
}

export function restoreChatHistory(events: SessionEvent[]): ChatHistoryState {
  const out: Message[] = [];
  // Index of the current assistant turn bubble; content appends into it until
  // a status/error event closes the turn.
  let open = -1;
  const closeOpen = () => {
    if (open >= 0 && out[open].role === 'assistant') {
      out[open].pending = false;
      out[open].closed = true;
    }
    open = -1;
  };
  const ensureOpen = (seedId: string): Message => {
    if (open >= 0 && !out[open].closed) {
      return out[open];
    }
    const msg: Message = { id: `${seedId}-turn`, role: 'assistant', blocks: [], pending: true };
    out.push(msg);
    open = out.length - 1;
    return msg;
  };
  for (const evt of events) {
    if (evt.type === 'user.message') {
      closeOpen();
      out.push({
        id: evt.id,
        role: 'user',
        blocks: [{ kind: 'text', id: evt.id, text: payloadText(evt.payload) }],
      });
    } else if (evt.type === 'agent.message') {
      const text = payloadText(evt.payload);
      if (text) {
        ensureOpen(evt.id).blocks.push({
          kind: 'text',
          id: evt.id,
          text,
          presentation: 'final',
        });
      }
    } else if (evt.type === 'agent.tool_use') {
      ensureOpen(evt.id).blocks.push({
        kind: 'tool',
        id: String(evt.payload?.id ?? evt.payload?.toolCallId ?? evt.payload?.toolUseId ?? evt.id),
        toolName: String(evt.payload?.name ?? evt.payload?.toolName ?? 'tool'),
        text: evt.payload?.input != null ? JSON.stringify(evt.payload.input) : undefined,
      });
    } else if (evt.type === 'agent.tool_result') {
      const toolUseId = String(
        evt.payload?.tool_use_id ?? evt.payload?.toolCallId ?? evt.payload?.id ?? '',
      );
      const output = evt.payload?.output != null
        ? String(evt.payload.output)
        : payloadText(evt.payload);
      if (!toolUseId) continue;
      for (const m of out) {
        const idx = m.blocks.findIndex(b => b.kind === 'tool' && b.id === toolUseId);
        if (idx >= 0) {
          m.blocks = m.blocks.map((b, i) => (i === idx ? { ...b, result: output } : b));
          break;
        }
      }
    } else if (evt.type === 'session.error') {
      closeOpen();
      out.push({
        id: evt.id,
        role: 'error',
        blocks: [{ kind: 'text', id: evt.id, text: errorText(evt) }],
      });
    } else if (evt.type.startsWith('session.status')) {
      // Status events do not end the turn bubble: the backend may emit
      // status_idle between model iterations of one user question, followed
      // by more tool calls and the final text. Only user.message / session.error
      // (or a later user.message) close the bubble.
      if (open >= 0 && out[open].role === 'assistant') {
        out[open].pending = false;
      }
    }
  }
  const openMessage = open >= 0 ? out[open] : undefined;
  if (openMessage?.role === 'assistant') {
    const displayState = createChatDisplayState();
    for (const block of openMessage.blocks) {
      if (block.kind === 'tool') {
        displayState.toolExecutions[block.id] = {
          id: block.id,
          toolName: block.toolName ?? 'tool',
          text: block.text,
          result: block.result,
        };
        displayState.displayOrder.push({
          key: uniqueDisplayKey(displayState, block.renderKey ?? block.id),
          kind: 'tool',
          refId: block.id,
        });
      } else if (block.text) {
        displayState.finalAnswer = {
          id: block.id,
          sourceId: block.sourceId ?? block.id,
          text: block.text,
          pending: false,
        };
        displayState.displayOrder.push({
          key: uniqueDisplayKey(displayState, block.id),
          kind: 'text',
          refId: block.id,
          presentation: 'final',
        });
      }
    }
    openMessage.blocks = [];
    openMessage.displayState = displayState;
  }
  return {
    messages: out,
    openMessageId: openMessage?.role === 'assistant' ? openMessage.id : null,
  };
}

export function applyAssistantFrameToHistory(
  history: ChatHistoryState,
  event: SessionEvent,
): ChatHistoryState {
  const current = history.openMessageId
    ? history.messages.find(message => message.id === history.openMessageId && !message.closed)
    : undefined;
  const displayState = applyChatFrame(
    current?.displayState ?? createChatDisplayState(),
    event,
  );
  const displayBlocks = chatDisplayBlocks(displayState);
  const attributes = { ...(event.payload ?? {}), ...(event.attributes ?? {}) };
  const settled = event.type === 'agent.message'
    || (event.type === 'event_update'
      && (attributes.disposition === 'TERMINAL'
        || (attributes.authoritative === true && attributes.hasOutput === false)));

  if (current) {
    if (displayBlocks.length === 0 && current.blocks.length === 0) {
      return {
        messages: history.messages.filter(message => message.id !== current.id),
        openMessageId: null,
      };
    }
    return {
      messages: history.messages.map(message => message.id === current.id
        ? {
            ...message,
            displayState,
            pending: settled ? false : event.type.startsWith('event_') ? true : message.pending,
          }
        : message),
      openMessageId: current.id,
    };
  }

  if (displayBlocks.length === 0) return history;
  const seedId = String(event.payload?.event_id ?? event.id ?? nextId());
  const messageId = `${seedId}-turn`;
  return {
    messages: [...history.messages, {
      id: messageId,
      role: 'assistant',
      blocks: [],
      displayState,
      pending: !settled,
    }],
    openMessageId: messageId,
  };
}
