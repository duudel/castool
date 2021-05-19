import React, { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import styled from 'styled-components';

const Lit = ({ children }: React.PropsWithChildren<Record<string, unknown>>) => {
  return <LitContainer>
    <Qt>"</Qt><LitElem>{children}</LitElem><Qt>"</Qt>
  </LitContainer>;
};

const Qt = styled.span`font-size: 12px`;

const Syn = ({ children }: React.PropsWithChildren<Record<string, unknown>>) => {
  return <SynElem>
    <Qt>{"<"}</Qt>{children}<Qt>{">"}</Qt>
  </SynElem>;
};

const Term = ({ children }: React.PropsWithChildren<Record<string, unknown>>) => {
  return <TermElem>
    <Syn>{children}</Syn>
    <RuleOp>::=</RuleOp>
  </TermElem>;
};

const Or = () => <MetaElem>|</MetaElem>;
const LParen = () => <MetaElem>(</MetaElem>;
const RParen = () => <MetaElem>)</MetaElem>;

export const RqlInstructions = () => (
  <Instructions>
    <p>
      Example query:
    </p>
    <pre>
{`test-table
| where car_id contains "127" and what-ever
| project car_id, model, prod_year
| order by prod_year desc
`}
    </pre>

    <p>
      Syntax:
    </p>

    <Rule>
      <Term>table-name</Term>
      <RuleSyntax>Formed by joining keyspace name and table name with underscore: [keyspace]_[table]</RuleSyntax>
    </Rule>
    <Rule>
      <Term>source</Term>
      <RuleSyntax>
        <Syn>table-name</Syn> <Or /> <LParen /> <Syn>source</Syn> <Lit>|</Lit> <Syn>toplevel-op</Syn> <RParen />
      </RuleSyntax>
    </Rule>
    <Rule>
      <Term>toplevel-op</Term>
      <RuleSyntax>
        <Syn>where-op</Syn> <Or />
        <Syn>project-op</Syn> <Or />
        <Syn>extend-op</Syn> <Or />
        <Syn>order-op</Syn> <Or />
        <Syn>summarize-op</Syn>
      </RuleSyntax>
    </Rule>
    <Rule>
      <Term>where-op</Term>
      <RuleSyntax>
        <Lit>where</Lit> <Syn>expr</Syn>
      </RuleSyntax>
    </Rule>
    <Rule>
      <Term>project-op</Term>
      <RuleSyntax>
        <Lit>project</Lit>
        <Syn>name-or-assign</Syn> [<Lit>,</Lit> <Syn>name-or-assign</Syn>]...
      </RuleSyntax>
    </Rule>
    <Rule>
      <Term>extend-op</Term>
      <RuleSyntax>
        <Lit>extend</Lit> <Syn>assign</Syn>
      </RuleSyntax>
    </Rule>
    <Rule>
      <Term>order-op</Term>
      <RuleSyntax>
        <Lit>order by</Lit>
        <Syn>name</Syn>[<Lit>,</Lit> <Syn>name</Syn>]...
        [ <Lit>asc</Lit><Or /><Lit>desc</Lit> ]
      </RuleSyntax>
    </Rule>
    <Rule>
      <Term>summarize-op</Term>
      <RuleSyntax>
        <Lit>summarize</Lit>
      </RuleSyntax>
    </Rule>
    <Rule>
      <Term>name</Term>
      column name
    </Rule>
    <Rule>
      <Term>number</Term>
      decimal number
    </Rule>
    <Rule>
      <Term>assign</Term>
      <Syn>name</Syn> <Lit>=</Lit> <Syn>expr</Syn>
    </Rule>
    <Rule>
      <Term>name-or-assign</Term>
      <RuleSyntax>
        <Syn>name</Syn> <Or /> <Syn>assign</Syn>
      </RuleSyntax>
    </Rule>
    <Rule>
      <Term>expr</Term>
      <Syn>term</Syn> [ <Syn>op</Syn> <Syn>expr</Syn> ]
    </Rule>
    <Rule>
      <Term>unary-expr</Term>
      <Syn>unary-op</Syn> <Syn>term</Syn>
    </Rule>
    <Rule>
      <Term>term</Term>
      <Lit>null</Lit> <Or /> <Lit>true</Lit> <Or /> <Lit>false</Lit> <Or />
      <Syn>number</Syn> <Or /> <Syn>name</Syn> <Or /> <Syn>function-call</Syn> <Or /> <Syn>unary-expr</Syn> <Or />
      <Lit>(</Lit> <Syn>expr</Syn> <Lit>)</Lit>
    </Rule>
    <Rule>
      <Term>op</Term>
      <Lit>+</Lit> <Or /> <Lit>-</Lit> <Or /> <Lit>*</Lit> <Or /> <Lit>/</Lit> <Or />
      <Lit>==</Lit> <Or /> <Lit>!=</Lit> <Or /> <Lit>{"<"}</Lit> <Or /> <Lit>{"<"}=</Lit> <Or /> <Lit>{">"}</Lit> <Or /> <Lit>{">"}=</Lit> <Or />
      <Lit>contains</Lit> <Or /> <Lit>!contains</Lit>
    </Rule>
    <Rule>
      <Term>unary-op</Term>
      <Lit>+</Lit> <Or /> <Lit>-</Lit> <Or /> <Lit>!</Lit>
    </Rule>
  </Instructions>
);

const Instructions = styled.div`padding: 8px;`;
const SynElem = styled.span`
  margin-left: 2px;
  margin-right: 2px;
  font-style: italic;
`;
const MetaElem = styled.span`
  margin-left: 10px;
  margin-right: 10px;
`;
const LitContainer = styled.span`
  /*margin-left: 5px;*/
  /*margin-right: 5px;*/
`;
const LitElem = styled.span`
  /*margin-left: 5px;
  margin-right: 5px;*/
  font-weight: bold;
`;
const Rule = styled.div`
  display: flex;
  flex-direction: row;
  margin-bottom: 4px;
`;
const TermElem = styled.div`
  display: flex;
  justify-content: flex-end;
  width: 200px;
`;
const RuleOp = styled.div`
  margin-left: 20px;
  margin-right: 20px;
`;
const RuleSyntax = styled.span``;

