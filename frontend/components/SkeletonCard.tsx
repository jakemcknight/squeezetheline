export function SkeletonCard() {
  return (
    <div className="rounded-xl border border-border bg-surface p-4 space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2.5">
          <div className="skeleton h-[34px] w-[34px] rounded-full" />
          <div className="space-y-1.5">
            <div className="skeleton h-4 w-28" />
            <div className="skeleton h-3 w-20" />
          </div>
        </div>
        <div className="skeleton h-6 w-20 rounded-md" />
      </div>
      <div className="flex items-end justify-between">
        <div className="space-y-1">
          <div className="skeleton h-3 w-14" />
          <div className="skeleton h-7 w-16" />
        </div>
        <div className="space-y-1">
          <div className="skeleton h-3 w-10" />
          <div className="skeleton h-7 w-16" />
        </div>
      </div>
      <div className="skeleton h-1.5 w-full rounded-full" />
      <div className="skeleton h-1.5 w-full rounded-full" />
    </div>
  );
}
