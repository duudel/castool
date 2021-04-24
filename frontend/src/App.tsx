import React from "react";
import { Dispatch, useCallback, useEffect, useReducer, useRef, useState } from "react";
import styled from 'styled-components';
import casLogo from "./logo/CasTool-2-icon.png";
import "./App.css";

import { useWebsocket } from './utils/UseWebsocketHook';
import Split from './Split';
import QuerySection from './QuerySection';
import { TransformerSection } from './TransformerSection';

import { Action, ActionType, reducer, initialState, State, QueryStatus } from './reducer';

import RqlQueryTab from './RqlQueryTab';

function renderTab(
  tab: number,
  state: State,
  dispatch: React.Dispatch<Action>,
  qref: React.RefObject<HTMLDivElement>,
  tref: React.RefObject<HTMLDivElement>,
  splitContainerRef: React.RefObject<HTMLDivElement>,
  sendQuery: (msg: any) => void,
) {
  switch (tab) {
    case 0:
      return (
        <SplitContainer ref={splitContainerRef}>
          <QuerySection forwardRef={qref} dispatch={dispatch} sendQuery={sendQuery} state={state} />
          <Split
            A={qref}
            B={tref}
            container={splitContainerRef}
            minPixelsA={150}
            minB={0.3}
          />
          <TransformerSection forwardRef={tref} state={state} dispatch={dispatch} />
        </SplitContainer>
      );
    case 1:
      return (
        <RqlQueryTab />
      );
  }
}

function App() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const dispatchAction = useCallback((message: MessageEvent<any>) => {
    dispatch({ type: ActionType.ON_MESSAGE, message: message.data })
  }, []);
  const [sendQuery, wsStatus] = useWebsocket("ws://localhost:8080/squery", dispatchAction);

  const [tab, setTab] = useState(0);

  const qref = useRef<HTMLDivElement | null>(null);
  const tref = useRef<HTMLDivElement | null>(null);
  const splitContainerRef = useRef<HTMLDivElement | null>(null);

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
        </TabButtonStrip>
      </Header>
      {renderTab(tab, state, dispatch, qref, tref, splitContainerRef, sendQuery)}
    </AppContainer>
  );
}

export default App;

const AppContainer = styled.div`
  overflow: scroll;
  height: 100vh;
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

  background-color: #282c34;
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

const TabButtonStrip = styled.div`
  margin-top: -10px;
`;

const TabButton = styled.button<{ selected: boolean }>`
  cursor: pointer;
  padding: 5px;
  background: #8f6f8f;
  ${({ selected }) => (selected ? "font-weight: bold" : "")};
  ${({ selected }) => (selected ? "background: #fff; border-bottom: 0" : "")};
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

