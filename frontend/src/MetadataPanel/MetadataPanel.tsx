import React from "react";
import styled from "styled-components";

import { metadataTree } from "../colors";
import { Metadata } from "../types";
import KeyspaceItem from "./KeyspaceItem";

interface MetadataPanelProps {
  data: Metadata | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

function ClusterInfo({ metadata }: { metadata: Metadata }) {
  return (
    <ClusterInfoContainer>
      <ClusterName>{metadata.clusterName}</ClusterName>
      <TopItem>Nodes</TopItem>
      <NodesContainer>
        {metadata.nodes.map(node =>
          <NodeInfo key={node.hostId}>
            <span>Host: {node.hostId}</span>
            <span>Datacenter: {node.datacenter}</span>
            <span>Rack: {node.rack}</span>
            <span>Endpoint: {node.endpoint}</span>
            <span>Schema version: {node.schemaVersion}</span>
            <span>Cassandra version: {node.cassandraVersion}</span>
          </NodeInfo>
        )}
      </NodesContainer>
    </ClusterInfoContainer>
  );
}

const ClusterName = styled.div``;
const NodesContainer = styled.div`
  display: flex;
  flex-direction: column;
  padding: 8px;
  background: ${metadataTree.background};
`;
const NodeInfo = styled.div`
  display: flex;
  flex-direction: column;
  padding: 8px;
  background: ${metadataTree.background};
`;

function MetadataPanel(props: MetadataPanelProps) {
  const { data: metadata, loading, error, refetch } = props;

  return (
    <Container>
      <RefreshButton onClick={refetch} disabled={loading}>
        {loading ? "Loading..." : "Refresh"}
      </RefreshButton>
      {error && error}
      {metadata && <ClusterInfo metadata={metadata} />}
      <TopItem>Keyspaces</TopItem>
      <KeyspacesContainer>
      {metadata && metadata.keyspaces.map(keyspace =>
        <KeyspaceItem key={"keyspace-" + keyspace.name} keyspace={keyspace} />
      )}
      </KeyspacesContainer>
    </Container>
  );
}

export default MetadataPanel;

const Container = styled.div`
  display: flex;
  flex-direction: column;
  padding: 8px;
  background-color: #fff;
  max-width: 300px;
`;

const ClusterInfoContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: baseline;
`;

const RefreshButton = styled.button`
  padding: 4px;
  margin-left: 10px;
  margin-right: 10px;
`;

const TopItem = styled.div`
  font-weight: bold;
  font-size: 16px;
`;

const KeyspacesContainer = styled.div`
  display: flex;
  flex-direction: column;
  padding: 8px;
  background: ${metadataTree.background};
  /*max-width: 200px;*/
  overflow: scroll;
`;

