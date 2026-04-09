import { Chat } from "@/components/Chat";

export default function Home() {
  return (
    <main className="relative flex h-screen flex-col">
      <header className="relative z-10 shrink-0 border-b border-white/5 bg-ballpark-panel/85 px-4 py-3 shadow-panel backdrop-blur-md">
        <div
          className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-ballpark-accent/70 to-transparent"
          aria-hidden
        />
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <p className="font-mono text-[10px] font-medium uppercase tracking-[0.2em] text-ballpark-accent/90">
              Statcast · FanGraphs
            </p>
            <h1 className="font-display text-lg font-semibold tracking-tight text-ballpark-chalk md:text-xl">
              Stats Masterson
            </h1>
            <p className="mt-1 max-w-xl text-xs leading-relaxed text-ballpark-chalk/65">
              Your dugout for numbers — ask in plain English, get Savant-backed tables and clear
              assumptions.
            </p>
          </div>
          <div className="hidden rounded-md border border-white/5 bg-ballpark-navy/50 px-2.5 py-1.5 text-[10px] text-ballpark-chalk/50 sm:block">
            <span className="font-mono text-ballpark-accent/90">pybaseball</span>
            <span className="mx-1.5 text-white/20">|</span>
            Table export
          </div>
        </div>
      </header>
      <Chat />
    </main>
  );
}
