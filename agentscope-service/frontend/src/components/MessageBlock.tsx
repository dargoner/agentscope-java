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

import React, { useMemo, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import ToolCallBlock from './ToolCallBlock';

/** Ordered content inside one assistant turn bubble (text and tool calls interleaved). */
export interface ContentBlock {
  kind: 'text' | 'tool';
  id: string;
  renderKey?: string;
  sourceId?: string;
  presentation?: 'commentary' | 'thinking' | 'pending' | 'preview' | 'final';
  /** Text content, or the tool-call input when kind === 'tool'. */
  text?: string;
  toolName?: string;
  result?: string;
}

export interface MessageBlockProps {
  role: 'user' | 'assistant' | 'system' | 'error';
  blocks: ContentBlock[];
  pending?: boolean;
  /** Soft auto-expand while actively streaming once the user opens it — not forced. */
  defaultOpen?: boolean;
}

const PREVIEW_CHARS = 120;
const EXPANDED_BODY_MAX_PX = 280;

const presentationLabels: Record<NonNullable<ContentBlock['presentation']>, string> = {
  commentary: 'Commentary',
  thinking: 'Thinking',
  pending: 'Draft',
  preview: 'Preview',
  final: 'Final answer',
};

function normalizePreview(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}

/** Single-line preview; while streaming show the trailing window (typewriter tip). */
export function messagePreviewLine(text: string, pending?: boolean): string {
  const t = normalizePreview(text);
  if (!t) {
    if (pending) return 'Generating…';
    return '';
  }
  if (t.length <= PREVIEW_CHARS) return t;
  // Pending: follow the typing tip; settled: keep the start of the message.
  return pending ? `…${t.slice(-PREVIEW_CHARS)}` : `${t.slice(0, PREVIEW_CHARS)}…`;
}

const roleStyles: Record<MessageBlockProps['role'], React.CSSProperties> = {
  user: {
    alignSelf: 'flex-end',
    background: 'linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%)',
    color: '#ffffff',
    boxShadow: '0 2px 6px rgba(99,102,241,0.25)',
    border: 'none',
  },
  assistant: {
    alignSelf: 'flex-start',
    background: '#ffffff',
    color: '#0f172a',
    border: '1px solid #e2e8f0',
    boxShadow: '0 1px 2px rgba(15,23,42,0.04)',
  },
  system: {
    alignSelf: 'center',
    background: 'transparent',
    color: '#94a3b8',
    border: '1px dashed #e2e8f0',
    fontStyle: 'italic',
  },
  error: {
    alignSelf: 'center',
    background: '#fef2f2',
    color: '#b91c1c',
    border: '1px solid #fecaca',
  },
};

const s: Record<string, React.CSSProperties> = {
  root: {
    maxWidth: '78%',
    borderRadius: 14,
    overflow: 'hidden',
    fontSize: '0.95rem',
    lineHeight: 1.6,
    flexShrink: 0,
    minHeight: 'fit-content',
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    width: '100%',
    padding: '10px 14px',
    border: 'none',
    background: 'transparent',
    cursor: 'pointer',
    textAlign: 'left',
    color: 'inherit',
    font: 'inherit',
    flexShrink: 0,
    minHeight: 40,
  },
  chevron: {
    flexShrink: 0,
    width: 14,
    fontSize: '0.75rem',
    opacity: 0.75,
  },
  role: {
    flexShrink: 0,
    fontSize: '0.72rem',
    fontWeight: 700,
    letterSpacing: '0.04em',
    textTransform: 'uppercase',
    opacity: 0.7,
  },
  preview: {
    flex: 1,
    minWidth: 0,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '0.85rem',
    opacity: 0.92,
  },
  meta: {
    flexShrink: 0,
    fontSize: '0.72rem',
    opacity: 0.65,
  },
  body: {
    padding: '0 14px 12px',
    display: 'flex',
    flexDirection: 'column',
    gap: 10,
    maxHeight: EXPANDED_BODY_MAX_PX,
    overflowY: 'auto',
    overscrollBehavior: 'contain',
  },
  text: {
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
  },
  pending: {
    color: '#94a3b8',
  },
  textBlock: {
    borderRadius: 8,
    padding: '8px 10px',
  },
  textLabel: {
    marginBottom: 4,
    color: '#64748b',
    fontSize: '0.7rem',
    fontWeight: 700,
    letterSpacing: '0.05em',
    textTransform: 'uppercase',
  },
};

function presentationStyle(presentation?: ContentBlock['presentation']): React.CSSProperties {
  switch (presentation) {
    case 'commentary':
      return { background: '#f8fafc', borderLeft: '3px solid #94a3b8', color: '#475569' };
    case 'thinking':
      return { background: '#f5f3ff', borderLeft: '3px solid #a78bfa', color: '#5b21b6' };
    case 'pending':
      return { background: '#eff6ff', borderLeft: '3px solid #60a5fa' };
    case 'preview':
      return { background: '#f8fafc', borderLeft: '3px solid #cbd5e1' };
    case 'final':
      return { background: '#ffffff', borderLeft: '3px solid #10b981' };
    default:
      return {};
  }
}

export default function MessageBlock({
  role,
  blocks,
  pending,
  defaultOpen = false,
}: MessageBlockProps) {
  const [open, setOpen] = useState(defaultOpen);
  const allText = useMemo(
    () => blocks.filter(b => b.kind === 'text').map(b => b.text ?? '').join(''),
    [blocks],
  );
  const previewText = useMemo(() => {
    const primary = blocks.filter(block => block.kind === 'text'
      && block.presentation !== 'commentary'
      && block.presentation !== 'thinking');
    if (primary.length > 0) return primary.map(block => block.text ?? '').join('');
    const context = [...blocks].reverse().find(block => block.kind === 'text');
    if (!context) return '';
    const label = context.presentation ? presentationLabels[context.presentation] : 'Context';
    return `${label}: ${context.text ?? ''}`;
  }, [blocks]);
  const toolCount = useMemo(() => blocks.filter(b => b.kind === 'tool').length, [blocks]);
  const preview = useMemo(() => messagePreviewLine(previewText, pending), [previewText, pending]);
  const toolHint = toolCount > 0 ? `${toolCount} tool${toolCount === 1 ? '' : 's'}` : '';

  const empty = blocks.length === 0;

  return (
    <div style={{ ...s.root, ...roleStyles[role] }}>
      <button
        type="button"
        style={s.header}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        <span style={s.chevron}>{open ? '▾' : '▸'}</span>
        <span style={s.role}>{role}</span>
        {!open && (
          <span style={s.preview} title={normalizePreview(previewText) || undefined}>
            {preview || (toolHint ? `${toolHint}…` : pending ? 'Generating…' : '(empty)')}
          </span>
        )}
        {!open && toolHint && preview && (
          <span style={s.meta}>{toolHint}</span>
        )}
        {open && <span style={{ flex: 1 }} />}
        {pending && <span style={s.meta}>streaming</span>}
      </button>

      {open && (
        <div style={s.body}>
          {blocks.map(b => (
            b.kind === 'text'
              ? (
                role === 'assistant'
                  ? (
                    <div
                      key={b.renderKey ?? b.id}
                      style={{ ...s.textBlock, ...presentationStyle(b.presentation) }}
                      data-presentation={b.presentation}
                    >
                      {b.presentation && (
                        <div style={s.textLabel}>{presentationLabels[b.presentation]}</div>
                      )}
                      <div className="md-text" style={{ wordBreak: 'break-word' }}>
                        <ReactMarkdown>{b.text}</ReactMarkdown>
                      </div>
                    </div>
                  )
                  : (
                    <div key={b.renderKey ?? b.id} style={s.text}>{b.text}</div>
                  )
              )
              : (
                <ToolCallBlock
                  key={b.renderKey ?? b.id}
                  toolName={b.toolName ?? 'tool'}
                  toolCallId={b.id}
                  input={b.text}
                  result={b.result}
                />
              )
          ))}
          {empty && pending && <div style={{ ...s.text, ...s.pending }}>…</div>}
          {empty && !pending && <div style={{ ...s.text, ...s.pending }}>—</div>}
          {!empty && pending && allText.trim() === '' && toolCount === 0 && (
            <div style={{ ...s.text, ...s.pending }}>…</div>
          )}
        </div>
      )}
    </div>
  );
}
