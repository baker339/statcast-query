"use client";

import { useCallback, useRef, useState } from "react";
import { AssistantMarkdown } from "./AssistantMarkdown";
import { MessageTable, type TablePayload } from "./MessageTable";
import { PulsingBaseball } from "./PulsingBaseball";

const SUGGESTED_QUESTION_PLACEHOLDER =
  'Try: "Who had the highest average fastball spin in 2024 among qualified pitchers?" — or ask about strikeouts, spin variance, and more.';

/** API / JSON may hand back Pydantic/Zod-shaped objects; React cannot render them as children. */
function safeForReactChild(v: unknown, fallback = ""): string {
  if (v === null || v === undefined) return fallback;
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  if (typeof v === "bigint") return String(v);
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return fallback || "[Unserializable value]";
  }
}

/** Prose overrides when the reply is flagged error but still Markdown (e.g. partial answer + data-service warning). */
const ASSISTANT_MARKDOWN_ERROR_CLASS =
  "!prose-p:text-red-200/95 !prose-strong:text-red-100 !prose-headings:text-red-50/95 !prose-li:text-red-200/90 !prose-code:text-red-100/95 !prose-a:text-red-300";

type ChatMessage = {
  id: string;
  role: "user" | "assistant";
  content: string;
  tables?: TablePayload[];
  assumptions?: string;
  error?: boolean;
};

type ProgressState = {
  title: string;
  detail: string;
};

export function Chat() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState<ProgressState | null>(null);
  const [progressSteps, setProgressSteps] = useState<string[]>([]);
  const [dataError, setDataError] = useState<string | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = useCallback(() => {
    requestAnimationFrame(() => bottomRef.current?.scrollIntoView({ behavior: "smooth" }));
  }, []);

  const send = async () => {
    const text = input.trim();
    if (!text || loading) return;

    setInput("");
    setDataError(null);
    const userMsg: ChatMessage = {
      id: crypto.randomUUID(),
      role: "user",
      content: text,
    };
    const history: { role: string; content: string }[] = [
      ...messages.map(({ role, content }) => ({ role, content })),
      { role: "user", content: text },
    ];
    setMessages((m) => [...m, userMsg]);
    setLoading(true);
    setProgress({ title: "Thinking", detail: "Sending your question…" });
    setProgressSteps([]);
    scrollToBottom();

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/x-ndjson",
        },
        body: JSON.stringify({ messages: history }),
      });

      const ct = res.headers.get("content-type") ?? "";

      if (!res.ok) {
        const data = (await res.json()) as {
          error?: string;
          rateLimited?: boolean;
        };
        if (data.rateLimited) {
          setDataError("Too many requests. Wait a minute and try again.");
        } else {
          const errLine = safeForReactChild(data.error, "");
          setDataError(errLine || `Request failed (${res.status})`);
        }
        setMessages((m) => [
          ...m,
          {
            id: crypto.randomUUID(),
            role: "assistant",
            content: safeForReactChild(data.error, "Something went wrong."),
            error: true,
          },
        ]);
        return;
      }

      if (!res.body || !ct.includes("ndjson")) {
        const data = (await res.json()) as {
          reply?: string;
          tables?: TablePayload[];
          assumptions?: string;
          error?: string;
        };
        if (data.error != null) setDataError(safeForReactChild(data.error));
        setMessages((m) => [
          ...m,
          {
            id: crypto.randomUUID(),
            role: "assistant",
            content: safeForReactChild(data.reply, ""),
            tables: data.tables,
            assumptions:
              data.assumptions === undefined || data.assumptions === null
                ? undefined
                : safeForReactChild(data.assumptions),
            error: Boolean(data.error),
          },
        ]);
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let final:
        | {
            reply?: string;
            tables?: TablePayload[];
            assumptions?: string;
            error?: string;
          }
        | undefined;
      let streamFailed = false;

      outer: while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          let ev: {
            type?: string;
            title?: string;
            detail?: string;
            message?: string;
            reply?: string;
            tables?: TablePayload[];
            assumptions?: string;
            error?: string;
          };
          try {
            ev = JSON.parse(trimmed) as typeof ev;
          } catch {
            continue;
          }
          if (ev.type === "status" && ev.title && ev.detail) {
            setProgress({ title: ev.title, detail: ev.detail });
            setProgressSteps((prev) => {
              const step = `${ev.title}: ${ev.detail}`;
              if (prev[prev.length - 1] === step) return prev;
              return [...prev, step].slice(-12);
            });
          } else if (ev.type === "error" && ev.message != null) {
            const errMsg = safeForReactChild(ev.message, "Request failed.");
            setDataError(errMsg);
            setMessages((m) => [
              ...m,
              {
                id: crypto.randomUUID(),
                role: "assistant",
                content: errMsg,
                error: true,
              },
            ]);
            final = undefined;
            streamFailed = true;
            break outer;
          } else if (ev.type === "done") {
            final = {
              reply: ev.reply,
              tables: ev.tables,
              assumptions: ev.assumptions,
              error: ev.error,
            };
          }
        }
      }

      if (streamFailed) {
        return;
      }

      if (final) {
        if (final.error != null) setDataError(safeForReactChild(final.error));
        setMessages((m) => [
          ...m,
          {
            id: crypto.randomUUID(),
            role: "assistant",
            content: safeForReactChild(final!.reply, ""),
            tables: final!.tables,
            assumptions:
              final!.assumptions === undefined || final!.assumptions === null
                ? undefined
                : safeForReactChild(final!.assumptions),
            error: Boolean(final!.error),
          },
        ]);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Network error";
      setDataError(msg);
      setMessages((m) => [
        ...m,
        {
          id: crypto.randomUUID(),
          role: "assistant",
          content: msg,
          error: true,
        },
      ]);
    } finally {
      setLoading(false);
      setProgress(null);
      setProgressSteps([]);
      scrollToBottom();
    }
  };

  const hasConversation = messages.length > 0 || loading;

  const composer = (
    <div
      className={
        hasConversation
          ? "shrink-0 border-t border-white/5 bg-ballpark-panel/40 p-3 backdrop-blur-sm"
          : "w-full max-w-3xl px-3 py-4"
      }
    >
      <div className="mx-auto flex max-w-3xl gap-3">
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              void send();
            }
          }}
          placeholder={
            hasConversation
              ? "Ask the next question — spin, Ks, barrels, variance…"
              : SUGGESTED_QUESTION_PLACEHOLDER
          }
          rows={hasConversation ? 2 : 5}
          disabled={loading}
          className="composer-field relative z-[1] min-h-[44px] flex-1 resize-y rounded-xl border border-white/[0.09] bg-ballpark-input px-3.5 py-2.5 text-sm text-ballpark-chalk/95 shadow-[0_14px_42px_-12px_rgba(0,0,0,0.72),0_6px_16px_-4px_rgba(0,0,0,0.45),0_0_0_1px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)] placeholder:text-ballpark-chalk/30 transition focus:border-ballpark-accent/40 focus:shadow-[0_16px_48px_-12px_rgba(0,0,0,0.65),0_6px_20px_-4px_rgba(0,0,0,0.4),0_0_0_1px_rgba(45,159,108,0.35),0_0_0_4px_rgba(45,159,108,0.12),inset_0_1px_0_rgba(255,255,255,0.07)] focus:outline-none focus:ring-0 disabled:opacity-50"
        />
        <button
          type="button"
          onClick={() => void send()}
          disabled={loading || !input.trim()}
          className="h-10 shrink-0 self-end rounded-xl bg-gradient-to-b from-ballpark-accent to-emerald-800 px-5 text-sm font-semibold text-white shadow-md shadow-black/30 transition hover:from-emerald-500 hover:to-emerald-700 disabled:cursor-not-allowed disabled:opacity-40 disabled:shadow-none"
        >
          {loading ? "…" : "Send"}
        </button>
      </div>
    </div>
  );

  return (
    <div className="relative z-[1] flex min-h-0 flex-1 flex-col">
      {!hasConversation ? (
        <div className="flex min-h-0 flex-1 flex-col items-center justify-center px-4">
          {dataError && (
            <div
              className="mb-4 w-full max-w-3xl rounded-lg border border-amber-500/25 bg-amber-950/30 px-3 py-2.5 text-xs text-amber-100/90 shadow-panel backdrop-blur-sm"
              role="alert"
            >
              {dataError}
            </div>
          )}
          {composer}
        </div>
      ) : (
        <>
          <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4">
            {dataError && (
              <div
                className="mb-4 rounded-lg border border-amber-500/25 bg-amber-950/30 px-3 py-2.5 text-xs text-amber-100/90 shadow-panel"
                role="alert"
              >
                {dataError}
              </div>
            )}
            <ul className="mx-auto flex max-w-3xl flex-col gap-5">
              {messages.map((msg) => (
                <li key={msg.id} className="text-sm">
                  <div className="flex items-center gap-2">
                    <span
                      className={`font-mono text-[10px] font-semibold uppercase tracking-[0.15em] ${
                        msg.role === "user" ? "text-ballpark-clay/90" : "text-ballpark-accent/90"
                      }`}
                    >
                      {msg.role === "user" ? "You" : "Stats Masterson"}
                    </span>
                    <span className="h-px flex-1 bg-gradient-to-r from-white/15 to-transparent" />
                  </div>
                  <div
                    className={`mt-2 rounded-r-lg border-l-2 py-1 pl-3 ${
                      msg.role === "user"
                        ? "border-ballpark-clay/50 bg-white/[0.02]"
                        : "border-ballpark-accent/50 bg-ballpark-accent-muted/30"
                    }`}
                  >
                    {msg.role === "user" ? (
                      <div className="whitespace-pre-wrap pr-2 text-ballpark-chalk/95">
                        {safeForReactChild(msg.content, "")}
                      </div>
                    ) : (
                      <AssistantMarkdown
                        content={safeForReactChild(msg.content, "")}
                        className={msg.error ? ASSISTANT_MARKDOWN_ERROR_CLASS : ""}
                      />
                    )}
                    {msg.assumptions && (
                      <p className="mt-2 border-t border-white/5 pt-2 text-xs italic text-ballpark-chalk/50">
                        Assumptions: {safeForReactChild(msg.assumptions, "")}
                      </p>
                    )}
                    {msg.tables?.map((t, i) => (
                      <MessageTable key={`${msg.id}-t-${i}`} table={t} />
                    ))}
                  </div>
                </li>
              ))}
              {loading && (
                <li className="text-sm">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-[10px] font-semibold uppercase tracking-[0.15em] text-ballpark-accent/90">
                      Stats Masterson
                    </span>
                    <span className="h-px flex-1 bg-gradient-to-r from-white/15 to-transparent" />
                  </div>
                  <div className="mt-2 rounded-r-lg border-l-2 border-ballpark-accent/50 bg-ballpark-accent-muted/20 py-3 pl-3 pr-3 text-ballpark-chalk/90">
                    <div className="flex gap-3">
                      <PulsingBaseball className="mt-0.5" />
                      <div className="min-w-0 flex-1 space-y-1.5">
                        <p className="text-[13px] font-semibold tracking-tight text-ballpark-chalk/95">
                          {safeForReactChild(progress?.title, "Thinking")}
                        </p>
                        <p className="text-xs leading-relaxed text-ballpark-chalk/60">
                          {safeForReactChild(progress?.detail, "Working on it…")}
                        </p>
                        {progressSteps.length > 0 && (
                          <ul className="mt-2 max-h-32 space-y-1 overflow-y-auto border-l border-white/10 pl-2.5 text-[11px] leading-snug text-ballpark-chalk/40">
                            {progressSteps.slice(-6).map((s, i) => (
                              <li key={`${i}-${s.slice(0, 48)}`} className="line-clamp-2">
                                {s}
                              </li>
                            ))}
                          </ul>
                        )}
                      </div>
                    </div>
                  </div>
                </li>
              )}
            </ul>
            <div ref={bottomRef} />
          </div>
          {composer}
        </>
      )}
    </div>
  );
}
