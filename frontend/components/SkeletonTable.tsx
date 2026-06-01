export function SkeletonTable({ rows = 6 }: { rows?: number }) {
  return (
    <div className="space-y-3">
      <div className="skeleton h-5 w-24" />
      <div className="overflow-x-auto rounded-xl border border-border bg-surface">
        <div className="min-w-[760px]">
          <div className="flex gap-4 border-b border-border px-4 py-3">
            {Array.from({ length: 9 }).map((_, i) => (
              <div key={i} className="skeleton h-3 w-16 flex-1" />
            ))}
          </div>
          {Array.from({ length: rows }).map((_, i) => (
            <div
              key={i}
              className="flex items-center gap-4 border-b border-border/60 last:border-0 px-4 py-3"
            >
              {Array.from({ length: 9 }).map((_, j) => (
                <div key={j} className="skeleton h-4 flex-1 rounded" />
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
