import React from 'react';
import { useCallback, useEffect, useRef, useState } from "react";
import styled from 'styled-components';

interface SplitProps {
  A: { current: HTMLElement | null },
  B: { current: HTMLElement | null },
  container: { current: HTMLElement | null },
  initialSplit?: number,
  minA?: number,
  minB?: number,
  minPixelsA?: number,
  minPixelsB?: number,
};

function clamp(a: number, x: number, b: number): number { return (x < a) ? a : (x > b) ? b : x; }

function processProps(props: SplitProps) {
  const { A, B, container, initialSplit, minA, minB, minPixelsA, minPixelsB } = props;
  const bounds = container.current ? container.current.getBoundingClientRect() : null;
  const getMinimumDimension = (ratio: number | undefined, pixels: number | undefined): number => {
    if (pixels !== undefined && bounds && bounds.height >= 1) {
      const result = clamp(0.0, pixels / bounds.height, 1.0);
      return result;
    } else if (ratio !== undefined) {
      return clamp(0.0, ratio, 1.0);
    } else {
      return 0.0;
    }
  };
  return {
    A,
    B,
    container,
    initialSplit,
    minA: getMinimumDimension(minA, minPixelsA),
    minB: getMinimumDimension(minB, minPixelsB)
  };
}

export default function Split(props: SplitProps) {
  const { A, B, container, initialSplit = 0.5, minA, minB } = processProps(props);

  const resizing = useRef(false);
  const [split, setSplit] = useState(initialSplit);

  const ref = useRef<HTMLDivElement | null>(null);

  const startResize = useCallback((ev: MouseEvent) => {
    ev.preventDefault();
    ev.stopPropagation();
    resizing.current = true;
  }, []);

  const calculateNewSplit = useCallback((clientY: number) => {
    if (!container.current) return split;
    const bounds = container.current.getBoundingClientRect();
    if (bounds.height >= 1) {
      const newSplit = (clientY - bounds.y) / bounds.height;
      const maxSplit = 1.0 - minB;
      const minSplit = 0.0 + minA;
      return clamp(minSplit, newSplit, maxSplit);
    }
    return split;
  }, [minA, minB, container, split]);

  const endResize = useCallback((ev: MouseEvent) => {
    if (resizing.current) {
      setSplit(calculateNewSplit(ev.clientY));
      resizing.current = false;
    }
  }, [calculateNewSplit]);

  const setReprPosition = useCallback((posPixels: number) => {
    if (ref.current && container.current) {
      const bounds = container.current.getBoundingClientRect();
      const halfRepr = ref.current.clientHeight / 2;
      ref.current.style.top = (bounds.y - halfRepr + posPixels) + "px";
    }
  }, [container]);

  const whileResize = useCallback((ev: MouseEvent) => {
    ev.preventDefault();
    ev.stopPropagation();
    if (resizing.current && ref.current && container.current) {
      const newSplit = calculateNewSplit(ev.clientY);
      const bounds = container.current.getBoundingClientRect();
      setReprPosition(newSplit * bounds.height);
    }
  }, [calculateNewSplit, setReprPosition, container]);

  const setPositions = useCallback((split: number) => {
    if (!container.current || !ref.current) return;
    const bounds = container.current.getBoundingClientRect();
    console.log("Bounds:", bounds);

    const reprHeight = ref.current.clientHeight;
    const halfRepr = reprHeight / 2;

    const heightA = (bounds.height - halfRepr) * split;
    const heightB = (bounds.height - halfRepr) * (1.0 - split);

    setReprPosition(bounds.height * split);
    if (A.current) {
      A.current.style.height = heightA + "px";
    }
    if (B.current) {
      B.current.style.marginTop = reprHeight + "px";
      B.current.style.height = heightB + "px";
    }
  }, [setReprPosition, A, B, container]);

  useEffect(() => {
    const refCurrent = ref.current;
    setPositions(split);
    if (ref.current) ref.current.addEventListener("mousedown", startResize);
    window.addEventListener("mouseup", endResize);
    window.addEventListener("mousemove", whileResize);
    return () => {
      if (refCurrent) refCurrent.removeEventListener("mousedown", startResize);
      window.removeEventListener("mouseup", endResize);
      window.removeEventListener("mousemove", whileResize);
    };
  }, [setPositions, startResize, endResize, whileResize, split]);

  return (
    <SplitRepr ref={ref}>- - -</SplitRepr>
  );
}

const SplitRepr = styled.div`
  position: absolute;
  z-index: 1000;
  height: 16px;
  width: 100%;

  display: flex;
  align-items: center;
  justify-content: center;
  border-top: 1px solid #888;
  border-bottom: 1px solid #888;

  cursor: ns-resize;
  background: #eee;
`;

