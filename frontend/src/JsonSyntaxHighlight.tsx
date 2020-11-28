import React from 'react';
import { useCallback, useEffect, useState } from 'react';

import { createParser, parseJson, ParserResults } from './json-parser';
import './JsonSyntaxHighlight.css';

interface JsonSyntaxHighlightProps {
  value: string;
  nopre?: boolean;
}

export function JsonSyntaxHighlight(props: JsonSyntaxHighlightProps) {
  const { value, nopre = false } = props;
  const [results, setResults] = useState<ParserResults>([]);
  const parse = useCallback((s: string) => {
    console.log("Re parse..");
    const parser = createParser(s);
    return parseJson(parser);
  }, []);
  useEffect(() => {
    const res = parse(value);
    setResults(res);
  }, [value, parse]);
  //const results = parse();
  const lineLimitReached = false;
  //const parser = createParser(value);
  //const results = parseJson(parser);
  //const lineLimitReached = parser.line >= parser.lineLimit;
  //console.log("parser:", parser);
  if (nopre) {
    return <div>
        {
            results.map(([s, cls], index) => {
                return <span className={cls} key={"sh" + index}>{s}</span>;
            })
        }
        {lineLimitReached && "..."}
    </div>
  } else {
    return <pre>
      {
          results.map(([s, cls], index) => {
              return <span className={cls} key={"sh" + index}>{s}</span>;
          })
      }
      {lineLimitReached && "..."}
    </pre>
  }
}

/*
export class JsonSyntaxHighlight extends React.Component<JsonSyntaxHighlightProps> {
    render() {
        const { value, nopre = false } = this.props;
        const parser = createParser(value);
        const results = parseJson(parser);
        const lineLimitReached = parser.line >= parser.lineLimit;
        //console.log("parser:", parser);
        if (nopre) {
            return <div>
                {
                    results.map(([s, cls], index) => {
                        return <span className={cls} key={"sh" + index}>{s}</span>;
                    })
                }
                {lineLimitReached && "..."}
            </div>
        }
        return <pre>
            {
                results.map(([s, cls], index) => {
                    return <span className={cls} key={"sh" + index}>{s}</span>;
                })
            }
            {lineLimitReached && "..."}
        </pre>
    }
}
*/
