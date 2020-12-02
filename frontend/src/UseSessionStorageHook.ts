import { useEffect, useState } from 'react';

export default function useSessionStorage<T>(key: string, initialValue: T): [T, (value: T) => void] {
  const [value, setValue] = useState(() => {
    try {
      const storedValue = window.sessionStorage.getItem(key);
      if (storedValue === null) {
        sessionStorage.setItem(key, JSON.stringify(initialValue));
        return initialValue;
      } else {
        return JSON.parse(storedValue);
      }
    } catch {
      return initialValue;
    }
  });

  useEffect(() => {
    try {
      const serialized = JSON.stringify(value);
      window.sessionStorage.setItem(key, serialized);
    } catch {
    }
  });

  return [value, setValue];
}

