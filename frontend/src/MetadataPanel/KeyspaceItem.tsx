import React, { useCallback, useMemo, useState } from "react";
import styled from "styled-components";

import { detailColor } from "../colors";
import { Keyspace, Table, dataTypeToString } from "../types";
import { TreeItem, TreeItemLine, TreeIcon, TreeItemIndent, TreeItemIndentBlank, TreeItemIndentLine, TreeItemIndentEmpty, TreeItemTitle, TreeItemsContainer } from "./TreeItem";

interface KeyspaceItemProps {
  keyspace: Keyspace;
}

function KeyspaceItem(props: KeyspaceItemProps) {
  const { keyspace } = props;
  const [open, setOpen] = useState(new Set());
  const isOpen = useCallback((id: string) => open.has(id), [open]);
  const toggleOpen = useCallback((id: string) => {
    const newSet = new Set(open);
    if (open.has(id)) {
      newSet.delete(id);
    } else {
      newSet.add(id);
    }
    setOpen(newSet);
  }, [open, setOpen]);

  const isKeyspaceOpen = useCallback((k: Keyspace) => isOpen(k.name), [isOpen]);
  const toggleKeyspace = useCallback((k: Keyspace) => toggleOpen(k.name), [toggleOpen]);

  const isTableOpen = useCallback((k: Keyspace, t: Table) => isOpen(k.name + "/" + t.name), [isOpen]);
  const toggleTable = useCallback((k: Keyspace, t: Table) => toggleOpen(k.name + "/" + t.name), [toggleOpen]);

  const heightOfTable = useCallback((t: Table) => {
    if (isTableOpen(keyspace, t)) {
      return t.columnDefs.length;
    } else {
      return 0;
    }
  }, [keyspace, isTableOpen]);

  const heightOfKeyspace = useMemo(() => {
    if (isKeyspaceOpen(keyspace)) {
      let items = keyspace.tables.length;
      keyspace.tables.forEach(t => {
        items += heightOfTable(t);
      });
      return items;
    } else {
      return 0;
    }
  }, [keyspace, isKeyspaceOpen, heightOfTable]);

  return (
    <TreeItem key={"keyspace-" + keyspace.name}>
      <TreeItemTitle>
        <TreeIcon onClick={() => toggleKeyspace(keyspace)}>{isKeyspaceOpen(keyspace) ? "-" : "+"}</TreeIcon>
        <b>{keyspace.name}</b>
      </TreeItemTitle>
      <TreeItemsContainer items={heightOfKeyspace}>
      {isKeyspaceOpen(keyspace) || false ? (
        <>
        {keyspace.tables.map((table, tableIndex) =>
          <TreeItem key={"table-" + table.name}>
            <TreeItemLine>
              <TreeItemIndent last={tableIndex + 1 === keyspace.tables.length}>
                <TreeItemIndentLine />
              </TreeItemIndent>
              <TreeItemTitle>
                <TreeIcon onClick={() => toggleTable(keyspace, table)}>{isTableOpen(keyspace, table) ? "-" : "+"}</TreeIcon>
                {table.name}
              </TreeItemTitle>
            </TreeItemLine>
            <TreeItemsContainer items={heightOfTable(table)}>
            {isTableOpen(keyspace, table) || false ? (
              <>
              {table.columnDefs.map((columnDef, index) =>
                <TreeItem key={"column-" + columnDef.name}>
                  <TreeItemLine>
                    <TreeItemIndentBlank last={tableIndex + 1 === keyspace.tables.length}>
                      <TreeItemIndentEmpty />
                    </TreeItemIndentBlank>
                    <TreeItemIndent last={index + 1 === table.columnDefs.length}>
                      <TreeItemIndentLine />
                    </TreeItemIndent>
                    <TreeItemTitle>
                      {columnDef.name}: <Detail>{dataTypeToString(columnDef.dataType)}</Detail>
                    </TreeItemTitle>
                  </TreeItemLine>
                </TreeItem>
              )}
              </>
            ) : null}
            </TreeItemsContainer>
          </TreeItem>
        )}
        </>
      ) : null}
      </TreeItemsContainer>
    </TreeItem>
  );
}

export default React.memo(KeyspaceItem);

const Detail = styled.div`
  font-style: italic;
  color: ${detailColor};
  margin-left: 10px;
`;

