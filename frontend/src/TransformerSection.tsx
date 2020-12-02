import React from 'react';
import styled from 'styled-components';

import { State } from './reducer';

import useSessionStorage from './UseSessionStorageHook';

function transform(row: any): any {
  if (row.event && typeof row.event === "string") {
    const json = JSON.parse(row.event)
    return json;
  }
  return ;
}

async function executeTransform(rowIterator: () => any, produce: (result: any) => void) {
  while (true) {
    const item = rowIterator();
    if (item === null) break;

    const result = transform(item);
    produce(result);
  }
}

interface TransformerSectionProps {
  forwardRef: { current: HTMLDivElement | null };
  state: State;
}

export function TransformerSection(props: TransformerSectionProps) {
  const { forwardRef, state } = props;
  const [script, setScript] = useSessionStorage("transform.script", "// Type script here");
  return (
    <Container ref={forwardRef}>
      <ScriptInput value={script} onChange={ev => setScript(ev.target.value)} />
    </Container>
  );
}

const Container = styled.div`
  overflow: scroll;
`;

const ScriptInput = styled.textarea``;

