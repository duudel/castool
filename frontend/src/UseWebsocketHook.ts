import { useCallback, useEffect, useRef, useState } from 'react';

export enum WebSocketStatus {
  CLOSED = "CLOSED",
  RECONNECTING = "RECONNECTING",
  CONNECTED = "CONNECTED",
}

const createWebSocket = (
  url: string,
  setReconnectionTimer: () => void,
  setStatus: (status: WebSocketStatus) => void,
  dispatch: (msg: MessageEvent<any>) => void
) => {
  console.log("Create new websocket");
  const ws = new WebSocket(url);

  ws.addEventListener("open", () => {
    setStatus(WebSocketStatus.CONNECTED);
  });

  ws.addEventListener("close", () => {
    setStatus(WebSocketStatus.CLOSED);
    setReconnectionTimer();
  });

  ws.addEventListener('message', dispatch);
  return ws;
}

export const useWebsocket = (
  url: string,
  dispatch: (msg: MessageEvent<any>) => void
): [(msg: any) => void, string] => {
  const ws = useRef<WebSocket | null>(null);
  const pending = useRef<any[]>([]);
  const [status, setStatus] = useState<WebSocketStatus>(WebSocketStatus.CLOSED);

  const send = useCallback((msg: any): void => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(msg);
    } else {
      pending.current.push(msg);
    }
  }, []);

  const setReconnectionTimer = () => {
    setStatus(WebSocketStatus.RECONNECTING);
    setTimeout(() => {
      if (ws.current === null || ws.current.readyState === WebSocket.CLOSED) {
        ws.current = createWebSocket(url, setReconnectionTimer, setStatus, dispatch);
      } else if (ws.current.readyState !== WebSocket.OPEN) {
        setReconnectionTimer();
      } else {
        setStatus(WebSocketStatus.CONNECTED);
      }
    }, 1000);
  };

  useEffect(() => {
    if (ws.current === null || ws.current.readyState === WebSocket.CLOSED) {
      ws.current = createWebSocket(url, setReconnectionTimer, setStatus, dispatch);
    }

    return () => {
      if (ws.current && ws.current.readyState !== WebSocket.CLOSED) {
        console.log("Closing websocket");
        ws.current.close();
        ws.current = null;
      }
    };
  }, [url]);
  useEffect(() => {
    if (ws.current && ws.current.onmessage !== dispatch) {
      console.log("Set message handler", dispatch);
      ws.current.addEventListener('message', dispatch);
      if (pending.current.length > 0) {
        const oldPending = pending.current;
        pending.current = [];
        oldPending.forEach(p => send(p));
      }
    }
  }, []);
  return [send, status];
};

