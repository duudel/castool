import React from 'react';

import { createParser, parseJson } from './json-parser';
import './JsonSyntaxHighlight.css';

interface JsonSyntaxHighlightProps {
  value: string;
  nopre?: boolean;
}

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

