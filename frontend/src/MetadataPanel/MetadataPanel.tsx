import React, { useCallback, useState } from "react";
import styled from "styled-components";

import { metadataTreeBackground, metadataTreeLines } from "../colors";
import { Metadata, Keyspace, Table } from "../types";
import KeyspaceItem from "./KeyspaceItem";

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
      <ClusterInfoContainer>
        {metadata && metadata.clusterName}
        <RefreshButton onClick={refetch}>Refresh</RefreshButton>
        {loading && "Loading..."}
      </ClusterInfoContainer>
      {error && error}
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
  padding: 8px
`;

const ClusterInfoContainer = styled.div`
  display: flex;
  flex-direction: row;
  align-items: baseline;
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
  background: ${metadataTreeBackground};
`;

