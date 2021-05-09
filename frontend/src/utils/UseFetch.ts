import React, { useCallback, useContext, useEffect, useRef, useState } from "react";

type CacheKey = string;

interface Metadata {
  initTime: Date;
  readyTime: Date;
  expiration: Date | null;
}

const expired = (meta: Metadata): boolean => {
  const timestamp = new Date();
  return meta.expiration !== null && meta.expiration.getTime() < timestamp.getTime();
};

interface FetchRequest {
  method: string;
  url: string;
  body?: string;
  expirationSeconds?: number;
  pollingInterval?: number;
}

interface FetchResponse {
  status: number;
  statusText: string;
  data: any;
}

function hashCode(s: string): number {
  let h = 1337 | 0;
  for(let i = 0, h = 0; i < s.length; i++)
      h = Math.imul(31, h) + s.charCodeAt(i) | 0;
  return h;
}

function requestKey(req: FetchRequest) {
  const { method, url, body } = req;
  return method + "_" + url + + ((body !== undefined) ? "_" + hashCode(body) : "");
}

class FetchCache {
  requests: Map<CacheKey, [Metadata, FetchRequest, FetchResponse]>;

  constructor() {
    this.requests = new Map();
  }

  get(request: FetchRequest, refetch?: boolean): Promise<FetchResponse> {
    const key = requestKey(request);
    const { method, url, body, expirationSeconds } = request;

    if (refetch !== true) {
      const result = this.requests.get(key);
      if (result) {
        const [meta, , response] = result;
        if (!expired(meta)) {
          return Promise.resolve(response);
        }

        this.requests.delete(key);
      }
    }

    const timestamp = new Date();

    return fetch(url, {
      method,
      body,
    })
    .then(response => {
      return response.json().then(json => {
        return {
          status: response.status,
          statusText: response.statusText,
          data: json
        } as FetchResponse;
      });
    })
    .then(response => {
      const expiration = expirationSeconds ? (
        new Date(timestamp.getTime() + expirationSeconds * 1000)
      ) : null;
      const meta = {
        initTime: timestamp,
        readyTime: new Date(),
        expiration
      };
      this.requests.set(key, [meta, request, response]);
      return response;
    })
  }
}

class Fetch {
  cache: FetchCache;
  constructor() {
    this.cache = new FetchCache();
  }

  fetch(request: FetchRequest, refetch?: boolean): Promise<FetchResponse> {
    return this.cache.get(request, refetch);
  }
}

export const context = new Fetch();

export const FetchContext = React.createContext<Fetch>(context);
FetchContext.displayName = "FetchContext";

export function useFetch<T>(request: FetchRequest): {
  data: T | null,
  loading: boolean,
  error: string | null,
  refetch: () => void,
  startPolling: (interval: number) => void,
  stopPolling: () => void,
} {
  const fc = useContext(FetchContext);
  const [result, setResult] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pollingInterval, setPollingInterval] = useState<number>(request.pollingInterval || 0);
  const [polling, setPolling] = useState((request.pollingInterval || 0) > 0);

  const doFetch = useCallback(async (refetch?: boolean) => {
    setLoading(true);
    const response = await fc.fetch(request, refetch)
      .catch(reason => setError(reason));
    setLoading(false);
    if (response === undefined) {
      console.log("json undefined");
      if (error === null) setError("Fetch failed!");
    } else {
      setResult(response.data as T);
    }
  }, [request, fc, error]);

  useEffect(() => {
    doFetch();
  }, []);

  const pollingTimeout = useRef<number | undefined>();

  useEffect(() => {
    if (polling && pollingInterval > 0) {
      const poll = () => {
        doFetch(true);
        doPolling();
      };
      const doPolling = () => {
        pollingTimeout.current = setTimeout(poll, pollingInterval);
      };
      doPolling();
    } else {
      clearTimeout(pollingTimeout.current);
    }
    return () => {
      clearTimeout(pollingTimeout.current);
    };
  }, [polling, pollingInterval, doFetch]);

  return {
    data: result,
    loading,
    error,
    refetch: () => doFetch(true),
    startPolling: (interval?: number) => {
      if (interval) setPollingInterval(interval);
      setPolling(true);
    },
    stopPolling: () => { setPolling(false) },
  };
}

