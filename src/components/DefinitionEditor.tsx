/**
 * DefinitionEditor — inline YAML editor for a job definition.
 *
 * Reconstructs the definition as editable YAML, lets the user modify it,
 * and PATCHes the changes back to jupyter-scheduler on Save.
 */

import React, { useCallback, useEffect, useState } from 'react';
import { IJobDefinition, updateJobDefinition } from '../api';

interface IProps {
  definition: IJobDefinition;
  onClose: () => void;
  onSaved: () => void;
}

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
  // Minimal parser: just enough to extract the fields we support editing.
  // We use a line-by-line approach to avoid a full YAML parser dependency.
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
        // schedule item start
        continue;
      }

      const kv = trimmed.match(/^(\w[\w_]*):\s*(.*)$/);
      if (kv && indent === 4) {
        // New key at schedule level — reset section flags first
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
  const [yaml, setYaml] = useState(() => definitionToYaml(definition));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  // Reset when definition changes
  useEffect(() => {
    setYaml(definitionToYaml(definition));
    setError(null);
    setSuccess(false);
  }, [definition.job_definition_id]);

  const handleSave = useCallback(async () => {
    setError(null);
    setSuccess(false);
    const patch = yamlToPath(yaml);
    console.log('[DefinitionEditor] parsed patch:', JSON.stringify(patch));
    if (!patch || !patch['schedule']) {
      setError('Could not parse YAML. Check the format and try again.');
      return;
    }
    setSaving(true);
    try {
      await updateJobDefinition(definition.job_definition_id, patch as Parameters<typeof updateJobDefinition>[1]);
      console.log('[DefinitionEditor] PATCH succeeded');
      setSuccess(true);
      setTimeout(() => onSaved(), 500);
    } catch (e: unknown) {
      console.error('[DefinitionEditor] PATCH failed:', e);
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
      <div className="marimo-def-editor-header">
        <span className="marimo-def-editor-title">
          Editing: <strong>{definition.name}</strong>
        </span>
        <button className="marimo-detail-close" onClick={onClose} title="Close editor">✕</button>
      </div>
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
    </div>
  );
}
