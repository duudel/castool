import React from "react";
import styled from "styled-components";

import { metadataTree } from "../colors";
import { Metadata } from "../types";
import ClusterInfo from "./ClusterInfo";
import KeyspaceItem from "./KeyspaceItem";
import { TopItem } from "./TreeItem";

interface MetadataPanelProps {
  data: Metadata | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

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
`;

const RefreshButton = styled.button`
  padding: 4px;
  margin-left: 10px;
  margin-right: 10px;
`;

const KeyspacesContainer = styled.div`
  display: flex;
  flex-direction: column;
  padding: 8px;
  background: ${metadataTree.background};
  overflow-x: scroll;
  width: 300px;
`;

