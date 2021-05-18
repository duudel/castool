import React, { useCallback, useState } from "react";
import styled from "styled-components";

import { metadataTree } from "../colors";
import { Metadata } from "../types";
import { TopItem, TreeItem, TreeItemTitle, TreeIcon } from "./TreeItem";

function remove<T>(set: Set<T>, x: T): Set<T> {
  const result = new Set(set);
  result.delete(x);
  return result;
}

function add<T>(set: Set<T>, x: T): Set<T> {
  const result = new Set(set);
  result.add(x);
  return result;
}

function ClusterInfo({ metadata }: { metadata: Metadata }) {
  const [open, setOpen] = useState(new Set());
  const isOpen = useCallback((node: string) => open.has(node), [open]);
  const toggle = useCallback((node: string) => setOpen(open.has(node) ? remove(open, node) : add(open, node)), [open, setOpen]);
  return (
    <ClusterInfoContainer>
      <ClusterName>{metadata.clusterName}</ClusterName>
      <TopItem>Nodes</TopItem>
      <NodesContainer>
        {metadata.nodes.map(node =>
          <TreeItem key={node.hostId}>
            <TreeItemTitle>
              <TreeIcon onClick={() => toggle(node.hostId)}>{isOpen(node.hostId) ? "-" : "+"}</TreeIcon>
              <b>{node.hostId}</b>
            </TreeItemTitle>
            <NodeInfo height={isOpen(node.hostId) ? 300 : 0}>
            {isOpen(node.hostId) && (
              <>
                <NodeInfoItem>
                  <ItemCaption>Host:</ItemCaption>
                  <ItemValue>{node.hostId}</ItemValue>
                </NodeInfoItem>
                <NodeInfoItem>
                  <ItemCaption>Datacenter:</ItemCaption>
                  <ItemValue>{node.datacenter}</ItemValue>
                </NodeInfoItem>
                <NodeInfoItem>
                  <ItemCaption>Rack:</ItemCaption>
                  <ItemValue>{node.rack}</ItemValue>
                </NodeInfoItem>
                <NodeInfoItem>
                  <ItemCaption>Endpoint:</ItemCaption>
                  <ItemValue>{node.endpoint}</ItemValue>
                </NodeInfoItem>
                <NodeInfoItem>
                  <ItemCaption>Schema version:</ItemCaption>
                  <ItemValue>{node.schemaVersion}</ItemValue>
                </NodeInfoItem>
                <NodeInfoItem>
                  <ItemCaption>Cassandra version:</ItemCaption>
                  <ItemValue>{node.cassandraVersion}</ItemValue>
                </NodeInfoItem>
              </>
            )}
            </NodeInfo>
          </TreeItem>
        )}
      </NodesContainer>
    </ClusterInfoContainer>
  );
}

export default ClusterInfo;

const ClusterInfoContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: baseline;
  width: 300px;
`;

const ClusterName = styled.div`
  display: flex;
  align-self: center;
  justify-items: center;
  padding: 5px;
  font-size: 16px;
  font-weight: bold;
`;
const NodesContainer = styled.div`
  display: flex;
  flex-direction: column;
  padding: 8px;
  background: ${metadataTree.background};
`;
const NodeInfo = styled.div<{ height: number }>`
  display: flex;
  flex-direction: column;
  height: ${({ height }) => `${height}px`};
  transition: height 200ms;
`;
const NodeInfoItem = styled.div`
  display: flex;
  flex-direction: column;
  margin-left: 8px;
  margin-top: 4px;
  margin-bottom: 4px;
`;
const ItemCaption = styled.div`
  font-weight: bold;
`;
const ItemValue = styled.div`
  margin-left: 10px;
`;

