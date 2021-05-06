import { useCallback, useEffect } from "react";

function useNotifyClickOutside<T extends HTMLElement>(ref: { current: T | null }, callback: () => void) {
  useEffect(() => {
    const handle = (ev: MouseEvent) => {
      const elem = ev.target as HTMLElement;
      if (ref.current && !ref.current.contains(elem)) {
        callback();
      }
    };
    document.addEventListener('mousedown', handle);
    return () => {
      document.removeEventListener('mousedown', handle);
    };
  }, [callback]);
}

export default useNotifyClickOutside;

