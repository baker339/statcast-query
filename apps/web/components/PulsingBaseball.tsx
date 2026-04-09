"use client";

/**
 * Loading indicator: green baseball with horseshoe seams (front view), not pinched “eye” curves.
 */
export function PulsingBaseball({ className = "" }: { className?: string }) {
  return (
    <span
      className={`relative inline-flex h-8 w-8 shrink-0 items-center justify-center ${className}`}
      aria-hidden
    >
      <span className="absolute inline-flex h-7 w-7 rounded-full bg-ballpark-accent/35 opacity-70 animate-ping" />
      <span className="absolute inline-flex h-5 w-5 rounded-full bg-ballpark-accent/25 animate-pulse" />
      <svg
        className="relative z-[1] h-[22px] w-[22px] drop-shadow-[0_0_10px_rgba(45,159,108,0.45)]"
        viewBox="0 0 32 32"
        xmlns="http://www.w3.org/2000/svg"
      >
        <circle cx="16" cy="16" r="15" className="fill-ballpark-accent" />
        {/* Left / right horseshoes: each bulges into the ball; open space in the middle (not an “almond eye”). */}
        <path
          d="M 8.5 6.5 Q 13.5 16 8.5 25.5"
          className="fill-none stroke-ballpark-chalk"
          strokeWidth="1.4"
          strokeLinecap="round"
        />
        <path
          d="M 23.5 6.5 Q 18.5 16 23.5 25.5"
          className="fill-none stroke-ballpark-chalk"
          strokeWidth="1.4"
          strokeLinecap="round"
        />
        {/* Stitches: short ticks crossing each seam. */}
        <g className="stroke-ballpark-chalk" strokeWidth="1.05" strokeLinecap="round">
          <line x1="9.2" y1="9.5" x2="11.6" y2="8.6" />
          <line x1="8.8" y1="12.5" x2="11.8" y2="11.8" />
          <line x1="8.6" y1="16" x2="12" y2="15.6" />
          <line x1="8.8" y1="19.5" x2="11.8" y2="20.2" />
          <line x1="9.3" y1="22.8" x2="11.6" y2="23.6" />
          <line x1="22.8" y1="9.5" x2="20.4" y2="8.6" />
          <line x1="23.2" y1="12.5" x2="20.2" y2="11.8" />
          <line x1="23.4" y1="16" x2="20" y2="15.6" />
          <line x1="23.2" y1="19.5" x2="20.2" y2="20.2" />
          <line x1="22.7" y1="22.8" x2="20.4" y2="23.6" />
        </g>
      </svg>
    </span>
  );
}
