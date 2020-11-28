import { useCallback, useEffect, useRef, useState } from 'react';

export const useWebsocket = (
  url: string,
  //receive: (event: MessageEvent<any>) => void
  dispatch: (msg: MessageEvent<any>) => void
): [(msg: any) => void] => {
  //const [reconnection, setReconnection] = useState(0);
  const ws = useRef<WebSocket | null>(null);
  const pending = useRef<any[]>([]);

  const send = useCallback((msg: any): void => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(msg);
    } else {
      pending.current.push(msg);
      //setReconnection(reconnection + 1);
    }
  }, []);

  useEffect(() => {
    if (ws.current === null || ws.current.readyState === WebSocket.CLOSED) {
      console.log("Create new websocket");
      ws.current = new WebSocket(url);
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
  return [send];
};

