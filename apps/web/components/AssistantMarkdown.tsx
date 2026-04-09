"use client";

import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

type Props = {
  content: string;
  className?: string;
};

/**
 * Renders assistant replies as GitHub-flavored Markdown (headings, lists, bold, etc.).
 */
export function AssistantMarkdown({ content, className = "" }: Props) {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      className={[
        "assistant-md max-w-none pr-2",
        "prose prose-invert prose-sm",
        "prose-headings:font-display prose-headings:font-semibold prose-headings:tracking-tight",
        "prose-h1:text-base prose-h1:mb-3 prose-h1:mt-0",
        "prose-h2:text-sm prose-h2:mb-2 prose-h2:mt-4 prose-h2:text-ballpark-chalk/95",
        "prose-p:leading-relaxed prose-p:text-ballpark-chalk/95",
        "prose-strong:text-ballpark-chalk prose-strong:font-semibold",
        "prose-ul:my-2 prose-ol:my-2 prose-li:my-0.5",
        "prose-li:marker:text-ballpark-accent/80",
        "prose-code:rounded prose-code:bg-black/30 prose-code:px-1 prose-code:py-0.5 prose-code:font-mono prose-code:text-[0.85em] prose-code:text-emerald-200/90",
        "prose-pre:bg-black/35 prose-pre:border prose-pre:border-white/10",
        "prose-blockquote:border-l-ballpark-accent/50 prose-blockquote:text-ballpark-chalk/75",
        className,
      ].join(" ")}
      components={{
        a: ({ href, children, ...rest }) => (
          <a
            href={href}
            {...rest}
            target="_blank"
            rel="noopener noreferrer"
            className="font-medium text-ballpark-accent underline decoration-ballpark-accent/40 underline-offset-2 hover:decoration-ballpark-accent"
          >
            {children}
          </a>
        ),
      }}
    >
      {content}
    </ReactMarkdown>
  );
}
