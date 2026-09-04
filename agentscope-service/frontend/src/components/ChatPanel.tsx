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

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Environment, listEnvironments } from '../api/environments';
import {
  EventStreamHandle,
  getManagedSession,
  listEvents,
  ManagedSession,
  postToolConfirmation,
  postUserMessage,
  SessionEvent,
  streamEvents,
} from '../api/managedSessions';
import MessageBlock, { ContentBlock } from './MessageBlock';

type Role = 'user' | 'assistant' | 'system' | 'error';

export interface Message {
  id: string;
  role: Role;
  blocks: ContentBlock[];
  displayState?: ChatDisplayState;
  pending?: boolean;
  /** Turn finished: no more blocks are appended to this bubble. */
  closed?: boolean;
}

interface PendingConfirmation {
  toolUseId: string;
  toolName: string;
  input?: Record<string, unknown>;
}

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

const NEAR_BOTTOM_PX = 96;

const S: Record<string, React.CSSProperties> = {
  root: { display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, background: '#f8fafc' },
  header: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '10px 28px', borderBottom: '1px solid #e2e8f0', background: '#ffffff',
    fontSize: '0.82rem', color: '#64748b', flexShrink: 0, flexWrap: 'wrap',
  },
  sessionTag: {
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace', fontSize: '0.78rem',
    background: '#f1f5f9', color: '#475569', padding: '2px 8px', borderRadius: 6,
  },
  iconBtn: {
    background: '#ffffff', border: '1px solid #e2e8f0', color: '#475569',
    padding: '5px 12px', borderRadius: 7, cursor: 'pointer', fontSize: '0.82rem', fontWeight: 500,
    textDecoration: 'none', display: 'inline-flex', alignItems: 'center',
  },
  thread: {
    flex: 1,
    overflowY: 'auto',
    padding: '28px 36px',
    display: 'flex',
    flexDirection: 'column',
    gap: 12,
    overscrollBehavior: 'auto',
  },
  empty: { color: '#94a3b8', fontSize: '0.95rem', textAlign: 'center', marginTop: 100 },
  confirmCard: {
    alignSelf: 'stretch', maxWidth: 520, margin: '0 auto',
    background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 12,
    padding: '16px 20px', boxShadow: '0 2px 8px rgba(146,64,14,0.08)',
    flexShrink: 0,
  },
  composer: {
    borderTop: '1px solid #e2e8f0', padding: '18px 28px',
    display: 'flex', gap: 12, background: '#ffffff',
  },
  textarea: {
    flex: 1, padding: '12px 16px',
    background: '#ffffff', border: '1px solid #cbd5e1', borderRadius: 10,
    color: '#0f172a', fontSize: '0.95rem', resize: 'none',
    minHeight: 48, maxHeight: 200, lineHeight: 1.55,
  },
  send: {
    padding: '0 24px',
    background: 'linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%)',
    color: '#ffffff', border: 'none',
    borderRadius: 10, cursor: 'pointer', fontSize: '0.95rem', fontWeight: 600,
    boxShadow: '0 2px 6px rgba(99,102,241,0.35), inset 0 1px 0 rgba(255,255,255,0.18)',
  },
  sendDisabled: { background: '#e2e8f0', color: '#94a3b8', cursor: 'not-allowed', boxShadow: 'none' },
  allowBtn: {
    padding: '8px 16px', background: '#059669', color: '#fff', border: 'none',
    borderRadius: 8, cursor: 'pointer', fontWeight: 600, fontSize: '0.88rem',
  },
  denyBtn: {
    padding: '8px 16px', background: '#ffffff', color: '#dc2626',
    border: '1px solid #fca5a5', borderRadius: 8, cursor: 'pointer', fontWeight: 600, fontSize: '0.88rem',
  },
};

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

function extractConfirmation(evt: SessionEvent): PendingConfirmation | null {
  if (evt.type === 'session.requires_action') {
    const p = evt.payload ?? {};
    const toolUseId = p.toolUseId != null ? String(p.toolUseId) : '';
    if (!toolUseId) return null;
    return {
      toolUseId,
      toolName: String(p.toolName ?? 'tool'),
      input: typeof p.input === 'object' && p.input != null ? p.input as Record<string, unknown> : undefined,
    };
  }
  if (evt.type === 'session.status_idle' || evt.type === 'session.status_requires_action') {
    const stopReason = evt.payload?.stopReason;
    if (stopReason && typeof stopReason === 'object') {
      const sr = stopReason as Record<string, unknown>;
      if (sr.toolUseId) {
        return {
          toolUseId: String(sr.toolUseId),
          toolName: String(sr.toolName ?? 'tool'),
          input: typeof sr.input === 'object' && sr.input != null ? sr.input as Record<string, unknown> : undefined,
        };
      }
    }
    if (evt.payload?.toolUseId) {
      return {
        toolUseId: String(evt.payload.toolUseId),
        toolName: String(evt.payload.toolName ?? 'tool'),
        input: typeof evt.payload.input === 'object' && evt.payload.input != null
          ? evt.payload.input as Record<string, unknown> : undefined,
      };
    }
  }
  return null;
}

function findScrollableParent(el: HTMLElement | null): HTMLElement | null {
  let node = el?.parentElement ?? null;
  while (node && node !== document.body) {
    const style = getComputedStyle(node);
    const oy = style.overflowY;
    if ((oy === 'auto' || oy === 'scroll' || oy === 'overlay')
      && node.scrollHeight > node.clientHeight + 1) {
      return node;
    }
    node = node.parentElement;
  }
  const root = document.scrollingElement;
  return root instanceof HTMLElement ? root : null;
}

function isNearBottom(el: HTMLElement, threshold = NEAR_BOTTOM_PX): boolean {
  return el.scrollHeight - el.scrollTop - el.clientHeight <= threshold;
}

/**
 * Chat bound to an existing Managed session. Does not create sessions —
 * POST user.message is the only turn driver.
 *
 * @param embedded — when true, hide session-hub navigation (for Team detail side panel).
 * @param readOnly — when true, hide composer mutations (e.g. completed team).
 */
export default function ChatPanel({
  sessionId,
  agentId,
  embedded = false,
  readOnly = false,
}: {
  sessionId: string;
  agentId: string;
  embedded?: boolean;
  readOnly?: boolean;
}) {
  const navigate = useNavigate();
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [restoring, setRestoring] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [managedSession, setManagedSession] = useState<ManagedSession | null>(null);
  const [envNameById, setEnvNameById] = useState<Map<string, string>>(new Map());
  const [pendingConfirm, setPendingConfirm] = useState<PendingConfirmation | null>(null);
  const threadRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLTextAreaElement | null>(null);
  const streamHandleRef = useRef<EventStreamHandle | null>(null);
  const pendingUserMsgIdRef = useRef<string | null>(null);
  /** Id of the current open assistant turn bubble; null when no turn is active. */
  const openMsgIdRef = useRef<string | null>(null);
  const seenEventIdsRef = useRef<Set<string>>(new Set());
  const lastSeqRef = useRef(0);
  /** When true, keep pinned to latest message as stream grows. */
  const stickToBottomRef = useRef(true);

  useEffect(() => {
    listEnvironments()
      .then((envs: Environment[]) => setEnvNameById(new Map(envs.map(e => [e.id, e.name]))))
      .catch(() => setEnvNameById(new Map()));
  }, []);

  const handleManagedEvent = useCallback((evt: SessionEvent) => {
    if (evt.id) {
      if (seenEventIdsRef.current.has(evt.id)) return;
      seenEventIdsRef.current.add(evt.id);
    }
    if (typeof evt.seq === 'number' && evt.seq > lastSeqRef.current) {
      lastSeqRef.current = evt.seq;
    }

    const confirm = extractConfirmation(evt);
    if (confirm) setPendingConfirm(confirm);

    const closeOpen = (prev: Message[]): Message[] => {
      const id = openMsgIdRef.current;
      openMsgIdRef.current = null;
      if (!id) return prev;
      return prev.map(m => (m.id === id ? { ...m, pending: false, closed: true } : m));
    };
    const updateDisplay = (prev: Message[]): Message[] => {
      const next = applyAssistantFrameToHistory(
        { messages: prev, openMessageId: openMsgIdRef.current },
        evt,
      );
      openMsgIdRef.current = next.openMessageId;
      return next.messages;
    };

    if (evt.type === 'event_start'
      || evt.type === 'event_delta'
      || evt.type === 'event_update'
      || evt.type === 'agent.message'
      || evt.type === 'agent.tool_use') {
      setMessages(updateDisplay);
      return;
    }

    if (evt.type === 'agent.tool_result') {
      const toolId = String(
        evt.payload?.tool_use_id ?? evt.payload?.toolCallId ?? evt.payload?.id ?? '',
      );
      if (!toolId) return;
      setMessages(prev => {
        const current = openMsgIdRef.current
          ? prev.find(message => message.id === openMsgIdRef.current && !message.closed)
          : undefined;
        if (current?.displayState?.toolExecutions[toolId]) {
          return updateDisplay(prev);
        }
        const result = evt.payload?.output != null
          ? String(evt.payload.output)
          : payloadText(evt.payload);
        let updated = false;
        const next = prev.map(message => {
          const index = message.blocks.findIndex(
            block => block.kind === 'tool' && block.id === toolId,
          );
          if (index < 0) return message;
          updated = true;
          return {
            ...message,
            blocks: message.blocks.map((block, i) =>
              i === index ? { ...block, result } : block),
          };
        });
        return updated ? next : prev;
      });
      return;
    }

    if (evt.type === 'user.message') {
      const text = payloadText(evt.payload);
      if (!text) return;
      const localUser = pendingUserMsgIdRef.current;
      pendingUserMsgIdRef.current = null;
      setMessages(prev => {
        let next = closeOpen(prev);
        if (next.some(m => m.id === evt.id)) return next;
        if (localUser && next.some(m => m.id === localUser)) {
          return next.map(m =>
            m.id === localUser
              ? { ...m, id: evt.id, blocks: [{ kind: 'text', id: evt.id, text }] }
              : m);
        }
        return [...next, { id: evt.id, role: 'user', blocks: [{ kind: 'text', id: evt.id, text }] }];
      });
      return;
    }

    if (evt.type.startsWith('session.status')) {
      // Keep the turn bubble open across status events (the backend may emit
      // status_idle between model iterations of one user question); only
      // user.message / session.error close it.
      setMessages(prev => {
        const id = openMsgIdRef.current;
        if (!id) return prev;
        return prev.map(m => (m.id === id ? { ...m, pending: false } : m));
      });
      return;
    }

    if (evt.type === 'session.error') {
      setMessages(prev => {
        const next = closeOpen(prev);
        if (next.some(m => m.id === evt.id)) return next;
        return [...next, {
          id: evt.id,
          role: 'error',
          blocks: [{ kind: 'text', id: evt.id, text: errorText(evt) }],
        }];
      });
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    setMessages([]);
    setInput('');
    setRestoring(true);
    setLoadError(null);
    setPendingConfirm(null);
    setManagedSession(null);
    seenEventIdsRef.current = new Set();
    lastSeqRef.current = 0;
    openMsgIdRef.current = null;
    pendingUserMsgIdRef.current = null;
    stickToBottomRef.current = true;
    streamHandleRef.current?.close();
    streamHandleRef.current = null;

    async function run() {
      try {
        const sess = await getManagedSession(sessionId);
        if (cancelled) return;
        setManagedSession(sess);
        const events = await listEvents(sessionId);
        if (cancelled) return;
        for (const e of events) {
          if (e.id) seenEventIdsRef.current.add(e.id);
          if (typeof e.seq === 'number' && e.seq > lastSeqRef.current) {
            lastSeqRef.current = e.seq;
          }
        }
        const history = restoreChatHistory(events);
        openMsgIdRef.current = history.openMessageId;
        setMessages(history.messages);
        streamHandleRef.current = streamEvents(
          sessionId,
          evt => { if (!cancelled) handleManagedEvent(evt); },
          () => { /* stream ended */ },
          {
            after: lastSeqRef.current,
            eventDeltas: ['agent.message', 'agent.thinking', 'agent.tool_use'],
            // Resume from the last seen sequence on automatic reconnects.
            getAfter: () => lastSeqRef.current,
            retryMs: 2000,
          },
        );
      } catch (e: unknown) {
        if (!cancelled) {
          setLoadError(e instanceof Error ? e.message : 'Failed to open session');
        }
      } finally {
        if (!cancelled) setRestoring(false);
      }
    }
    void run();
    return () => {
      cancelled = true;
      streamHandleRef.current?.close();
      streamHandleRef.current = null;
    };
  }, [sessionId, handleManagedEvent]);

  useEffect(() => {
    const el = threadRef.current;
    if (!el || !stickToBottomRef.current) return;
    el.scrollTop = el.scrollHeight;
  }, [messages, pendingConfirm]);

  function handleThreadScroll() {
    const el = threadRef.current;
    if (!el) return;
    stickToBottomRef.current = isNearBottom(el);
  }

  /**
   * When the thread is already at an edge, forward wheel deltas to the outer
   * page scroller so nested overflow does not trap scroll-up during streaming.
   */
  function handleThreadWheel(e: React.WheelEvent<HTMLDivElement>) {
    const el = threadRef.current;
    if (!el) return;
    const atTop = el.scrollTop <= 0;
    const atBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - 1;
    const scrollingUp = e.deltaY < 0;
    const scrollingDown = e.deltaY > 0;
    if ((scrollingUp && atTop) || (scrollingDown && atBottom)) {
      const parent = findScrollableParent(el);
      if (parent && parent !== el) {
        parent.scrollTop += e.deltaY;
      }
    }
  }

  const canSend = useMemo(
    () =>
      !readOnly &&
      !busy &&
      !restoring &&
      !loadError &&
      !pendingConfirm &&
      input.trim().length > 0,
    [readOnly, busy, restoring, loadError, pendingConfirm, input],
  );

  const mountLabel = useMemo(() => {
    if (!managedSession) return null;
    const env = envNameById.get(managedSession.environmentId) || managedSession.environmentId || '—';
    const vaults = managedSession.vaultIds?.length ?? 0;
    const mems = managedSession.memoryStoreIds?.length ?? 0;
    return `env: ${env} · vaults: ${vaults} · memory: ${mems}`;
  }, [managedSession, envNameById]);

  /**
   * Relabels the optimistic user bubble with the server event id so the same event
   * arriving over the stream reconciles instead of appending a twin. The stream is
   * deliberately NOT pre-deduped: the user.message handler must run so it closes the
   * previous turn bubble before the next reply is appended.
   */
  function adoptRecordedUserEvent(recorded: SessionEvent[]) {
    const localUser = pendingUserMsgIdRef.current;
    if (!localUser) return;
    const serverEvent = recorded.find(e => e.type === 'user.message' && e.id);
    if (!serverEvent) return;
    pendingUserMsgIdRef.current = null;
    if (typeof serverEvent.seq === 'number' && serverEvent.seq > lastSeqRef.current) {
      lastSeqRef.current = serverEvent.seq;
    }
    setMessages(prev =>
      prev.some(m => m.id === serverEvent.id)
        ? prev.filter(m => m.id !== localUser)
        : prev.map(m => (m.id === localUser ? { ...m, id: serverEvent.id } : m)));
  }

  async function handleSend() {
    if (!canSend) return;
    const text = input.trim();
    setInput('');
    setBusy(true);
    stickToBottomRef.current = true;
    const userMsg: Message = {
      id: nextId(),
      role: 'user',
      blocks: [{ kind: 'text', id: nextId(), text }],
    };
    pendingUserMsgIdRef.current = userMsg.id;
    setMessages(prev => [...prev, userMsg]);

    try {
      const recorded = await postUserMessage(sessionId, text);
      adoptRecordedUserEvent(recorded);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'send failed';
      setMessages(prev => [...prev, {
        id: nextId(),
        role: 'system',
        blocks: [{ kind: 'text', id: nextId(), text: `[error] ${msg}` }],
      }]);
      pendingUserMsgIdRef.current = null;
    } finally {
      setBusy(false);
      inputRef.current?.focus();
    }
  }

  async function handleConfirmation(allow: boolean) {
    if (readOnly || !pendingConfirm) return;
    setBusy(true);
    try {
      await postToolConfirmation(
        sessionId,
        pendingConfirm.toolUseId,
        allow,
        allow ? undefined : 'Denied by user',
      );
      setPendingConfirm(null);
      stickToBottomRef.current = true;
      setMessages(prev => [...prev, {
        id: nextId(),
        role: 'system',
        blocks: [{
          kind: 'text',
          id: nextId(),
          text: allow ? `Tool "${pendingConfirm.toolName}" allowed.` : `Tool "${pendingConfirm.toolName}" denied.`,
        }],
      }]);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'confirmation failed';
      setMessages(prev => [...prev, {
        id: nextId(),
        role: 'system',
        blocks: [{ kind: 'text', id: nextId(), text: `[error] ${msg}` }],
      }]);
    } finally {
      setBusy(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  }

  function handleNewChat() {
    if (busy) return;
    navigate(`/sessions/new?agentId=${encodeURIComponent(agentId)}`);
  }

  const sessionLabel = sessionId.slice(0, 24);

  if (loadError) {
    return (
      <div style={S.root}>
        <div style={S.empty}>
          {loadError}
          {!embedded && (
            <div style={{ marginTop: 16, display: 'flex', gap: 12, justifyContent: 'center' }}>
              <Link to="/sessions" style={{ ...S.iconBtn, color: '#6366f1' }}>Sessions</Link>
              <Link
                to={`/sessions/new?agentId=${encodeURIComponent(agentId)}`}
                style={{ ...S.iconBtn, color: '#6366f1' }}
              >
                New session
              </Link>
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div style={S.root}>
      <div style={S.header}>
        <span>{embedded ? 'Team chat' : 'Managed session'}</span>
        <span style={S.sessionTag} title={sessionId}>
          {restoring ? 'resolving…' : sessionLabel}{sessionId.length > 24 ? '…' : ''}
        </span>
        {!embedded && mountLabel && (
          <Link
            to={`/sessions/${encodeURIComponent(sessionId)}?tab=details`}
            style={{ ...S.iconBtn, maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
            title="View / edit mounts on Details"
          >
            {mountLabel}
          </Link>
        )}
        <span style={{ flex: 1 }} />
        {!embedded && (
          <>
            <Link
              to={`/sessions/${encodeURIComponent(sessionId)}?tab=details`}
              style={S.iconBtn}
              title="Session details and event timeline"
            >
              📊 Details
            </Link>
            <Link to="/sessions" style={S.iconBtn}>
              📋 All sessions
            </Link>
            <button type="button" style={S.iconBtn} onClick={handleNewChat} disabled={busy}>
              ✨ New session
            </button>
          </>
        )}
        {embedded && (
          <Link
            to={`/sessions/${encodeURIComponent(sessionId)}`}
            style={S.iconBtn}
            title="Open full session page"
          >
            Full page
          </Link>
        )}
      </div>
      <div
        style={S.thread}
        ref={threadRef}
        onScroll={handleThreadScroll}
        onWheel={handleThreadWheel}
      >
        {restoring && messages.length === 0 && <div style={S.empty}>Loading conversation…</div>}
        {!restoring && messages.length === 0 && (
          <div style={S.empty}>
            Session ready. Send a message to start the first turn — events stay empty until then.
          </div>
        )}
        {(() => {
          // Auto-expand the latest assistant turn bubble (even when a follow-up
          // user message sits after it) so replies and tool calls are readable.
          let lastAssistant = -1;
          for (let i = messages.length - 1; i >= 0; i--) {
            if (messages[i].role === 'assistant') {
              lastAssistant = i;
              break;
            }
          }
          return messages.map((m, i) => (
            <MessageBlock
              key={m.id}
              role={m.role}
              blocks={m.displayState ? chatDisplayBlocks(m.displayState) : m.blocks}
              pending={m.pending}
              defaultOpen={i === lastAssistant}
            />
          ));
        })()}
        {pendingConfirm && !readOnly && (
          <div style={S.confirmCard}>
            <div style={{ fontWeight: 700, color: '#92400e', marginBottom: 8 }}>
              Allow tool call: {pendingConfirm.toolName}?
            </div>
            {pendingConfirm.input && (
              <pre style={{
                fontSize: '0.78rem', color: '#78350f', background: '#fef3c7',
                padding: '8px 10px', borderRadius: 6, overflow: 'auto', maxHeight: 120,
              }}>
                {JSON.stringify(pendingConfirm.input, null, 2)}
              </pre>
            )}
            <div style={{ display: 'flex', gap: 10, marginTop: 12 }}>
              <button type="button" style={S.allowBtn} onClick={() => handleConfirmation(true)} disabled={busy}>Allow</button>
              <button type="button" style={S.denyBtn} onClick={() => handleConfirmation(false)} disabled={busy}>Deny</button>
            </div>
          </div>
        )}
      </div>
      <div style={S.composer}>
        <textarea
          ref={inputRef}
          style={S.textarea}
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={
            readOnly
              ? 'Read-only transcript — sending is disabled'
              : restoring
                ? 'Loading…'
                : pendingConfirm
                  ? 'Confirm tool call above…'
                  : `Message ${agentId}…`
          }
          rows={1}
          autoFocus={!readOnly}
          disabled={readOnly || restoring || !!pendingConfirm}
        />
        <button
          style={{ ...S.send, ...(canSend ? {} : S.sendDisabled) }}
          onClick={handleSend}
          disabled={!canSend}
        >
          {busy ? '…' : 'Send'}
        </button>
      </div>
    </div>
  );
}
