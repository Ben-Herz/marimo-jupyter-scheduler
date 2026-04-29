/**
 * DefinitionEditor — inline editor for a job definition.
 *
 * Tabs:
 *   YAML   — edit the schedule YAML and save changes back to jupyter-scheduler
 *   Flow   — Mermaid dependency graph of the Marimo notebook
 */

import React, { useCallback, useEffect, useState } from 'react';
import { IJobDefinition, updateJobDefinition } from '../api';
import { NotebookFlowDiagram } from './NotebookFlowDiagram';

interface IProps {
  definition: IJobDefinition;
  onClose: () => void;
  onSaved: () => void;
}

type EditorTab = 'yaml' | 'flow';

function definitionToYaml(def: IJobDefinition): string {
  const params = def.parameters && Object.keys(def.parameters).length > 0
    ? Object.entries(def.parameters)
        .map(([k, v]) => `      ${k}: "${v}"`)
        .join('\n')
    : null;

  const tags = def.tags && def.tags.length > 0
    ? def.tags.map(t => `      - ${t}`).join('\n')
    : null;

  const formats = (def.output_formats ?? ['html'])
    .map(f => `      - ${f}`)
    .join('\n');

  return [
    `version: "1"`,
    ``,
    `schedules:`,
    `  - name: ${def.name}`,
    `    notebook: ${def.input_filename}`,
    `    cron: "${def.schedule}"`,
    `    timezone: "${def.timezone || 'UTC'}"`,
    `    output_formats:`,
    formats,
    params ? `    parameters:\n${params}` : null,
    tags ? `    tags:\n${tags}` : null,
    `    enabled: ${def.active ? 'true' : 'false'}`,
  ]
    .filter(line => line !== null)
    .join('\n');
}

function yamlToPath(yaml: string): Record<string, unknown> | null {
  try {
    const lines = yaml.split('\n');
    const result: Record<string, unknown> = {};
    let inParams = false;
    let inTags = false;
    let inFormats = false;
    const params: Record<string, string> = {};
    const tags: string[] = [];
    const formats: string[] = [];

    for (const line of lines) {
      const trimmed = line.trimStart();
      const indent = line.length - trimmed.length;

      if (indent === 4 && trimmed.startsWith('- ') && !inParams && !inTags && !inFormats) {
        continue;
      }

      const kv = trimmed.match(/^(\w[\w_]*):\s*(.*)$/);
      if (kv && indent === 4) {
        inParams = false; inTags = false; inFormats = false;
        const [, key, val] = kv;
        const v = val.replace(/^["']|["']$/g, '');
        switch (key) {
          case 'name':           result['name'] = v; break;
          case 'notebook':       result['input_filename'] = v; break;
          case 'cron':           result['schedule'] = v; break;
          case 'timezone':       result['timezone'] = v; break;
          case 'enabled':        result['active'] = v === 'true'; break;
          case 'parameters':     inParams = true; break;
          case 'tags':           inTags = true; break;
          case 'output_formats': inFormats = true; break;
        }
      } else if (indent === 6 && inFormats && trimmed.startsWith('- ')) {
        formats.push(trimmed.slice(2).trim());
      } else if (indent === 6 && inTags && trimmed.startsWith('- ')) {
        tags.push(trimmed.slice(2).trim());
      } else if (indent === 6 && inParams) {
        const m = trimmed.match(/^(\w+):\s*["']?(.+?)["']?$/);
        if (m) params[m[1]] = m[2];
      }
    }

    if (formats.length > 0) result['output_formats'] = formats;
    if (tags.length > 0) result['tags'] = tags;
    if (Object.keys(params).length > 0) result['parameters'] = params;

    return result;
  } catch {
    return null;
  }
}

export function DefinitionEditor({ definition, onClose, onSaved }: IProps): JSX.Element {
  const [activeTab, setActiveTab] = useState<EditorTab>('yaml');
  const [yaml, setYaml] = useState(() => definitionToYaml(definition));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  useEffect(() => {
    setYaml(definitionToYaml(definition));
    setError(null);
    setSuccess(false);
  }, [definition.job_definition_id]);

  const handleSave = useCallback(async () => {
    setError(null);
    setSuccess(false);
    const patch = yamlToPath(yaml);
    if (!patch || !patch['schedule']) {
      setError('Could not parse YAML. Check the format and try again.');
      return;
    }
    setSaving(true);
    try {
      await updateJobDefinition(definition.job_definition_id, patch as Parameters<typeof updateJobDefinition>[1]);
      setSuccess(true);
      setTimeout(() => onSaved(), 500);
    } catch (e: unknown) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }, [yaml, definition.job_definition_id, onSaved]);

  const handleReset = useCallback(() => {
    setYaml(definitionToYaml(definition));
    setError(null);
    setSuccess(false);
  }, [definition]);

  return (
    <div className="marimo-def-editor">
      {/* Header with title, tab bar, and close button */}
      <div className="marimo-def-editor-header">
        <span className="marimo-def-editor-title">
          Editing: <strong>{definition.name}</strong>
        </span>
        <div className="marimo-def-editor-tabs">
          <button
            className={`marimo-def-editor-tab${activeTab === 'yaml' ? ' marimo-def-editor-tab--active' : ''}`}
            onClick={() => setActiveTab('yaml')}
          >
            YAML
          </button>
          <button
            className={`marimo-def-editor-tab${activeTab === 'flow' ? ' marimo-def-editor-tab--active' : ''}`}
            onClick={() => setActiveTab('flow')}
          >
            Flow Diagram
          </button>
        </div>
        <button className="marimo-detail-close" onClick={onClose} title="Close editor">✕</button>
      </div>

      {/* YAML tab */}
      {activeTab === 'yaml' && (
        <>
          <textarea
            className="marimo-scheduler-yaml-editor"
            value={yaml}
            onChange={e => { setYaml(e.target.value); setSuccess(false); }}
            spellCheck={false}
            rows={18}
          />
          <div className="marimo-def-editor-actions">
            <button
              className="marimo-scheduler-btn marimo-scheduler-btn--primary"
              onClick={() => void handleSave()}
              disabled={saving}
            >
              {saving ? 'Saving…' : 'Save changes'}
            </button>
            <button className="marimo-scheduler-btn" onClick={handleReset} disabled={saving}>
              Reset
            </button>
            {error && <span className="marimo-scheduler-error-inline">⚠ {error}</span>}
            {success && <span className="marimo-scheduler-success-inline">✓ Saved</span>}
          </div>
        </>
      )}

      {/* Flow Diagram tab */}
      {activeTab === 'flow' && (
        <div className="marimo-flow-tab-content">
          <NotebookFlowDiagram inputFilename={definition.input_filename} />
        </div>
      )}
    </div>
  );
}
