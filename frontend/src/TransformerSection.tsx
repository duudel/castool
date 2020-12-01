import React from 'react';
import styled from 'styled-components';

interface TransformerProps {
}

export function Transformer(props: TransformerProps) {
  return <TransformerContainer>Transformer shit</TransformerContainer>;
}

const TransformerContainer = styled.div`
  display: flex;
  flex-direction: column;
`;

