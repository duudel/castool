import styled from "styled-components";

import { metadataTreeBackground, metadataTreeLines } from "../colors";

const TreeItem_Height = 22;

export const TreeItemLine = styled.div`
  display: flex;
  flex-direction: row;
  align-items: flex-end;
  margin: 0;
  padding: 0;
  height: ${TreeItem_Height}px;
`;

export const TreeItemTitle = styled.div`
  display: flex;
  flex-direction: row;
  align-items: baseline;
  padding: 2px;
`;

export const TreeItemIndent = styled.div<{ last: boolean }>`
  margin-left: 0.5em;
  height: ${p => p.last ? "10px" : "22px"};
  margin-bottom: ${p => p.last ? "12px" : "0"};
  border-left: 1px solid ${metadataTreeLines};
`;

export const TreeItemIndentBlank = styled.div<{ last: boolean }>`
  width: 25px;
  margin-left: 0.5em;
  height: ${p => p.last ? "10px" : "22px"};
  border-left: 1px solid ${metadataTreeLines};
`;

export const TreeItemIndentLine = styled.div`
  width: 25px;
  margin-top: 10px;
  border-bottom: 1px solid ${metadataTreeLines};
`;

export const TreeIcon = styled.button`
  box-sizing: border-box;
  width: 1em;
  height: 1em;
  padding: 0;
  margin: 0;
  margin-right: 10px;
  border: 1px solid black;
  background: white;
  cursor: pointer;
`;

export const TreeItem = styled.div`
  display: flex;
  flex-direction: column;
`;

export const TreeItemsContainer = styled.div<{ items: number }>`
  display: flex;
  flex-direction: column;
  height: ${p => p.items * TreeItem_Height}px;
  transition: height 100ms linear;
  overflow: hidden;
`;

