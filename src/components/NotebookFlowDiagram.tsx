/**
 * NotebookFlowDiagram — renders a Mermaid flowchart of a Marimo notebook's
 * cell dependency graph.
 *
 * Parsing rules:
 *   - Import-only cells (bodies consisting entirely of import/from statements
 *     and private `_var` assignments) are hidden — they clutter the graph.
 *     Their exports still wire up other cells.
 *   - SQL cells (`mo.sql(...)`) get a cylinder DB node for the engine they use,
 *     with an arrow from the DB into the SQL cell.
 *   - All other visible cells are plain rectangles.
 *   - Edges are labelled with the variable that flows between cells.
 */

import React, { useEffect, useRef, useState } from 'react';
import { fetchFileContent } from '../api';

// ─── Marimo parser ────────────────────────────────────────────────────────────

interface MarimoCell {
  id: string;
  funcName: string;
  params: string[];
  returns: string[];
  label: string;
  /** Cell body is only imports / private assignments — hide from graph */
  isImportOnly: boolean;
  /** Cell calls mo.sql() */
  isSqlCell: boolean;
  /** Variable name of the engine passed to mo.sql() */
  sqlEngine: string | null;
  /** First meaningful SQL keyword line for the label */
  sqlLabel: string | null;
}

/** Returns true when the cell is pure setup/infrastructure with no
 *  user-visible output — imports, variable assignments, and engine creation.
 *  Uses an explicit allowlist rather than a catch-all regex. */
function checkImportOnly(bodyLines: string[]): boolean {
  // Control-flow keywords that definitely make a cell interesting
  const BLOCK_KEYWORDS = ['with ', 'for ', 'while ', 'if ', 'try:', 'raise ', 'yield ', 'assert '];

  for (const line of bodyLines) {
    const t = line.trim();
    if (!t || t.startsWith('#') || t.startsWith('return')) continue;

    // Imports
    if (t.startsWith('import ') || t.startsWith('from ')) continue;

    // Control flow → always interesting
    if (BLOCK_KEYWORDS.some(kw => t.startsWith(kw))) return false;

    // Closing bracket / continuation line
    if (/^[)\]},]/.test(t)) continue;

    // Assignment: the line must have `=` that is NOT `==`, `!=`, `<=`, `>=`
    // and the left-hand side must be only word chars, underscores, commas, spaces
    // (covers: x=, _x=, CONST=, x, y =)
    const eqIdx = t.indexOf('=');
    if (eqIdx > 0) {
      const charBefore = t[eqIdx - 1];
      const charAfter = t[eqIdx + 1] ?? '';
      const isComparison = charAfter === '=' || '!<>'.includes(charBefore);
      if (!isComparison) {
        const lhs = t.slice(0, eqIdx);
        if (/^[A-Za-z_][\w\s,]*$/.test(lhs)) continue;
      }
    }

    // Anything else: bare function call, chained call, etc. → interesting
    return false;
  }
  return true;
}

/** Extract `engine=<varname>` or first identifier arg to mo.sql(). */
function extractSqlEngine(block: string): string | null {
  // Prefer keyword arg:  engine=some_var
  const kwMatch = block.match(/engine\s*=\s*(\w+)/);
  if (kwMatch) return kwMatch[1];
  return null;
}

/** Return the first meaningful SELECT/INSERT/… line from the SQL string. */
function extractSqlLabel(block: string): string | null {
  // Find the triple-quoted string inside mo.sql(...)
  const sqlMatch = block.match(/mo\.sql\s*\(\s*(?:f?"""([\s\S]*?)"""|f?'''([\s\S]*?)''')/);
  const raw = sqlMatch ? (sqlMatch[1] ?? sqlMatch[2] ?? '') : '';
  for (const line of raw.split('\n')) {
    const t = line.trim();
    if (t && !/^\$\{/.test(t)) {
      return t.length > 52 ? t.slice(0, 52) + '…' : t;
    }
  }
  return null;
}

function parseMarimoNotebook(source: string): MarimoCell[] {
  const cells: MarimoCell[] = [];
  const blocks = source.split(/(?=@app\.cell)/);

  let idx = 0;
  for (const block of blocks) {
    if (!block.trim().startsWith('@app.cell')) continue;

    const defMatch = block.match(/def\s+(\w+)\s*\(([^)]*)\)\s*:/);
    if (!defMatch) continue;

    const funcName = defMatch[1];
    const paramsStr = defMatch[2].trim();
    const params = paramsStr
      ? paramsStr.split(',').map(p => p.trim()).filter(Boolean)
      : [];

    // Return values — strip outer parens and trailing comma
    const returnMatch = block.match(/\n[ \t]+return\s+([^\n]+)/);
    let returns: string[] = [];
    if (returnMatch) {
      const retStr = returnMatch[1]
        .trim()
        .replace(/^\(/, '').replace(/\)$/, '').replace(/,$/, '').trim();
      if (retStr) {
        returns = retStr
          .split(',')
          .map(r => r.trim())
          .filter(r => r && r !== 'None' && /^\w+$/.test(r));
      }
    }

    // Body lines (everything after `def ...:` and before end of block)
    const allLines = block.split('\n');
    const defLineIdx = allLines.findIndex(l => /def\s+\w+\s*\(/.test(l));
    const bodyLines = defLineIdx >= 0 ? allLines.slice(defLineIdx + 1) : [];

    const isImportOnly = checkImportOnly(bodyLines);

    // SQL cell detection
    const isSqlCell = block.includes('mo.sql(');
    const sqlEngine = isSqlCell ? extractSqlEngine(block) : null;
    const sqlLabel = isSqlCell ? extractSqlLabel(block) : null;

    // Build visible label
    let label = '';
    if (isSqlCell) {
      label = sqlLabel
        ? sqlLabel.replace(/["\[\]{}<>]/g, "'")
        : 'SQL query';
    } else if (funcName !== '_' && funcName !== '__') {
      label = funcName;
    } else {
      // First meaningful body line
      for (const line of bodyLines) {
        const t = line.trim();
        if (t && !t.startsWith('#') && !t.startsWith('return')) {
          label = t.slice(0, 48).replace(/["\[\]{}<>]/g, "'");
          if (t.length > 48) label += '…';
          break;
        }
      }
    }
    if (!label) label = `cell_${idx}`;

    cells.push({
      id: `cell_${idx}`,
      funcName,
      params,
      returns,
      label,
      isImportOnly,
      isSqlCell,
      sqlEngine,
      sqlLabel,
    });
    idx++;
  }

  return cells;
}

function generateMermaid(cells: MarimoCell[]): string {
  if (cells.length === 0) {
    return 'flowchart TD\n    empty["No Marimo cells found"]';
  }

  // Map variable name → cell id (including hidden import cells so edges resolve)
  const varToCell = new Map<string, string>();
  for (const cell of cells) {
    for (const ret of cell.returns) {
      varToCell.set(ret, cell.id);
    }
  }

  const visible = cells.filter(c => !c.isImportOnly);

  const lines: string[] = ['flowchart TD'];

  // Collect unique DB nodes needed (engine var → db node id)
  const engineNodes = new Map<string, string>(); // engineVarName → node id
  for (const cell of visible) {
    if (cell.isSqlCell && cell.sqlEngine) {
      if (!engineNodes.has(cell.sqlEngine)) {
        engineNodes.set(cell.sqlEngine, `db_${cell.sqlEngine}`);
      }
    }
  }

  // DB cylinder nodes
  for (const [engineVar, nodeId] of engineNodes) {
    lines.push(`    ${nodeId}[("${engineVar}")]`);
  }

  // Cell nodes
  for (const cell of visible) {
    lines.push(`    ${cell.id}["${cell.label}"]`);
  }

  // Edges between cells
  const added = new Set<string>();
  for (const cell of visible) {
    for (const param of cell.params) {
      const src = varToCell.get(param);
      if (src && src !== cell.id) {
        // Only draw if source is also visible (or is a DB node)
        const srcCell = cells.find(c => c.id === src);
        if (srcCell && srcCell.isImportOnly) continue; // skip hidden import cells
        const key = `${src}->${cell.id}:${param}`;
        if (!added.has(key)) {
          lines.push(`    ${src} -->|${param}| ${cell.id}`);
          added.add(key);
        }
      }
    }
  }

  // DB → SQL cell edges
  for (const cell of visible) {
    if (cell.isSqlCell && cell.sqlEngine) {
      const dbId = engineNodes.get(cell.sqlEngine);
      if (dbId) {
        lines.push(`    ${dbId} -->|query| ${cell.id}`);
      }
    }
  }

  return lines.join('\n');
}

// ─── Component ────────────────────────────────────────────────────────────────

let renderCount = 0;

export function NotebookFlowDiagram({
  inputFilename,
}: {
  inputFilename: string;
}): JSX.Element {
  const [svg, setSvg] = useState<string | null>(null);
  const [mermaidSrc, setMermaidSrc] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const renderId = useRef(`marimo-flow-${++renderCount}`);

  useEffect(() => {
    let cancelled = false;
    setSvg(null);
    setMermaidSrc(null);
    setError(null);
    setLoading(true);

    if (!inputFilename.endsWith('.py')) {
      setError('Flow diagram is only available for Marimo (.py) notebooks.');
      setLoading(false);
      return;
    }

    const path = inputFilename.replace(/^\/+/, '');

    fetchFileContent(path)
      .then(async content => {
        if (cancelled) return;
        const cells = parseMarimoNotebook(content);
        const src = generateMermaid(cells);
        if (cancelled) return;
        setMermaidSrc(src);

        const { default: mermaid } = await import('mermaid');
        mermaid.initialize({
          startOnLoad: false,
          theme: 'neutral',
          securityLevel: 'strict',
          flowchart: { useMaxWidth: true, htmlLabels: false },
        });
        const { svg: rendered } = await mermaid.render(renderId.current, src);
        if (!cancelled) setSvg(rendered);
      })
      .catch(err => {
        if (!cancelled) setError(`Could not load diagram: ${String(err)}`);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [inputFilename]);

  if (loading) {
    return <div className="marimo-flow-loading">Loading notebook…</div>;
  }

  if (error) {
    return <div className="marimo-flow-error">{error}</div>;
  }

  return (
    <div className="marimo-flow-container">
      {svg && (
        <div
          className="marimo-flow-svg"
          dangerouslySetInnerHTML={{ __html: svg }}
        />
      )}
      {mermaidSrc && (
        <details className="marimo-flow-source-details">
          <summary>Mermaid source</summary>
          <pre className="marimo-flow-source">{mermaidSrc}</pre>
        </details>
      )}
    </div>
  );
}
