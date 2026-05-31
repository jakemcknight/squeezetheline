"use client";

import { useEffect, useState } from "react";

interface State<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
}

/**
 * Minimal data-fetching hook. `deps` re-runs the fetcher (e.g. when the
 * selected sport or date changes). Avoids pulling in SWR/React-Query for
 * this small app.
 */
export function useApi<T>(
  fetcher: () => Promise<T>,
  deps: unknown[]
): State<T> {
  const [state, setState] = useState<State<T>>({
    data: null,
    loading: true,
    error: null,
  });

  useEffect(() => {
    let cancelled = false;
    setState((s) => ({ ...s, loading: true, error: null }));
    fetcher()
      .then((data) => {
        if (!cancelled) setState({ data, loading: false, error: null });
      })
      .catch((err: unknown) => {
        if (!cancelled)
          setState({
            data: null,
            loading: false,
            error: err instanceof Error ? err.message : "Request failed",
          });
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return state;
}
