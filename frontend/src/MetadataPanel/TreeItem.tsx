import styled from "styled-components";

import { metadataTree } from "../colors";

const TreeItem_Height = 24;

export const TreeItem = styled.div`
  display: flex;
  flex-direction: column;
`;

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
  align-self: flex-start;
  align-items: center;
  padding: 2px 8px;
  margin-top: 2px;
  border-radius: 4px;
  background: ${metadataTree.item};
`;

export const TreeItemIndent = styled.div<{ last: boolean }>`
  margin-left: 14px;
  height: ${p => p.last ? "10" : TreeItem_Height}px;
  margin-bottom: ${p => p.last ? "14px" : "0"};
  border-left: 1px solid ${metadataTree.lines};
`;

export const TreeItemIndentBlank = styled.div<{ last: boolean }>`
  width: 25px;
  margin-left: 14px;
  height: ${p => p.last ? "10" : TreeItem_Height}px;
  ${p => !p.last ? `border-left: 1px solid ${metadataTree.lines};` : ""}
`;

export const TreeItemIndentLine = styled.div`
  width: 25px;
  margin-top: 10px;
  border-bottom: 1px solid ${metadataTree.lines};
`;

export const TreeIcon = styled.button`
  box-sizing: border-box;
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  width: 14px;
  height: 14px;
  padding: 0;
  margin: 0;
  margin-right: 10px;
  border: 1px solid black;
  background: white;
  cursor: pointer;
`;

export const TreeItemsContainer = styled.div<{ items: number }>`
  display: flex;
  flex-direction: column;
  height: ${p => p.items * TreeItem_Height}px;
  transition: height 100ms linear;
  overflow: hidden;
`;

