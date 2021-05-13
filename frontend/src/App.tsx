import React from "react";
import { Dispatch, useCallback, useReducer, useState } from "react";
import styled from 'styled-components';
import casLogo from "./logo/CasTool-2-icon.png";
import "./App.css";

import { useWebsocket } from './utils/UseWebsocketHook';
import { useFetch } from './utils/UseFetch';
import QuerySection from './QuerySection';

import { Action, ActionType, reducer, initialState, State, QueryStatus } from './reducer';

import RqlQueryTab from './RqlQueryTab';
import { Metadata } from "./types";
import MetadataPanel from "./MetadataPanel";
import { headerBackground, mainTab } from "./colors";

function renderTab(
  tab: number,
  state: State,
  dispatch: React.Dispatch<Action>,
  sendQuery: (msg: any) => void,
  metadata: { data: Metadata | null, loading: boolean, error: string | null, refetch: () => void },
) {
  switch (tab) {
    case 0:
      return (
        <SplitContainer>
          <QuerySection dispatch={dispatch} sendQuery={sendQuery} state={state} />
          {/*<TransformerSection state={state} dispatch={dispatch} />*/}
        </SplitContainer>
      );
    case 1:
      return <RqlQueryTab />;
    case 2:
      return <MetadataPanel {...metadata} />
  }
}

function App() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const dispatchAction = useCallback((message: MessageEvent<any>) => {
    dispatch({ type: ActionType.ON_MESSAGE, message: message.data })
  }, []);
  const [sendQuery, wsStatus] = useWebsocket("ws://localhost:8080/squery", dispatchAction);
  const metadata = useFetch<Metadata>({
    method: "GET",
    url: window.location.protocol + "//" + window.location.host + "/metadata",
    expirationSeconds: 5000
  });

  console.log("Response ", metadata);

  const [tab, setTab] = useState(0);

  const logoIdle = state.queryStatus === QueryStatus.Done;

  return (
    <AppContainer>
      <Header>
        <HeaderMain>
          <AppLogo src={casLogo} alt="cassandra-tool-logo" idle={logoIdle} />
          <h1>Cassandra Tool</h1>
          <ConnectionInfo>{wsStatus}</ConnectionInfo>
        </HeaderMain>
        <TabButtonStrip>
          <TabButton selected={tab === 0} onClick={() => setTab(0)}>CQL Query</TabButton>
          <TabButton selected={tab === 1} onClick={() => setTab(1)}>RQL Query</TabButton>
          <TabButton selected={tab === 2} onClick={() => setTab(2)}>Metadata</TabButton>
        </TabButtonStrip>
        <TabStripBottom />
      </Header>
      <Content>
        <MetadataPanel {...metadata} />
        <TabContainer>
          {renderTab(tab, state, dispatch, sendQuery, metadata)}
        </TabContainer>
      </Content>
    </AppContainer>
  );
}

export default App;

const AppContainer = styled.div`
  overflow: scroll;
  height: 100vh;
`;

const Content = styled.div`
  display: flex;
  flex-direction: row;
`;

const TabContainer = styled.div`
  width:100%;
  height:100%;
`;

const AppLogo = styled.img<{ idle: boolean }>`
  height: 10vmin;
  pointer-events: none;
  animation: ${({ idle }) =>
    (idle ? "App-logo-blink infinite 16s linear" : "App-logo-exec infinite 1.6s linear")
  }
`;

const Header = styled.header`
  position: sticky;
  left: 0;

  background-color: ${headerBackground};
  height: 12vh;
  display: flex;
  flex-direction: column;

  font-family: "Courier";
  font-style: italic;
  font-size: calc(8px + 1vmin);
  color: white;
`;

const HeaderMain = styled.header`
  background-color: #282c34;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;

  font-family: "Courier";
  font-style: italic;
  font-size: calc(8px + 1vmin);
  color: white;
`;

const TabStripBottom = styled.div`
  height: 5px;
  background: #fff;
`;

const TabButtonStrip = styled.div`
  display: flex;
  flex-direction: row;
  margin-top: -18px;
`;

const TabButton = styled.button<{ selected: boolean }>`
  display: flex;
  flex-direction: row;
  padding: 8px;
  background: ${mainTab.unselectedBackground};
  height: 32px;
  border: 1px solid ${mainTab.border};
  border-radius: 4px;
  font-size: 12px;
  ${({ selected }) => (selected ? "font-weight: bold" : "")};
  ${({ selected }) => (selected ? `background: ${mainTab.selectedBackground}; border-bottom: 0` : "")};
  cursor: pointer;
`;

const ConnectionInfo = styled.div`
  position: absolute;
  top: 0;
  right: 0;
  padding: 10px;
`;

const SplitContainer = styled.div`
  height: 88vh;
`;

