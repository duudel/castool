import { useCallback, useEffect, useRef, useState } from 'react';

const createWebsocket = (url: string, setReconnectionTimer: () => void, setStatus: (status: string) => void, dispatch: (msg: MessageEvent<any>) => void) => {
  console.log("Create new websocket");
  const ws = new WebSocket(url);

  ws.addEventListener("open", () => {
    setStatus("OPEN");
  });

  ws.addEventListener("close", () => {
    setStatus("CLOSED");
    setReconnectionTimer();
  });

  ws.addEventListener('message', dispatch);
  return ws;
}

export const useWebsocket = (
  url: string,
  dispatch: (msg: MessageEvent<any>) => void
): [(msg: any) => void] => {
  const ws = useRef<WebSocket | null>(null);
  const pending = useRef<any[]>([]);
  const [status, setStatus] = useState("CLOSED");

  const send = useCallback((msg: any): void => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(msg);
    } else {
      pending.current.push(msg);
    }
  }, []);

  const setReconnectionTimer = () => {
    setStatus("RECONNECTING");
    setTimeout(() => {
      if (ws.current === null || ws.current.readyState === WebSocket.CLOSED) {
        console.log("reconnect");
        //ws.current = new WebSocket(url);
        ws.current = createWebsocket(url, setReconnectionTimer, setStatus, dispatch);
      } else if (ws.current.readyState !== WebSocket.OPEN) {
        setReconnectionTimer();
      } else {
        setStatus("CONNECTED");
      }
    }, 1000);
  };

  useEffect(() => {
    if (ws.current === null || ws.current.readyState === WebSocket.CLOSED) {
      //console.log("Create new websocket");
      //ws.current = new WebSocket(url);
      ws.current = createWebsocket(url, setReconnectionTimer, setStatus, dispatch);
    }

    //ws.current.addEventListener("open", () => {
    //  setStatus("OPEN");
    //});

    //ws.current.addEventListener("close", () => {
    //  setReconnectionTimer();
    //});

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
  return [send];
};

