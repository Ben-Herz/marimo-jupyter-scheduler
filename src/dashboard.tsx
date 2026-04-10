/**
 * Marimo Scheduler Dashboard — main React component.
 *
 * Sections:
 *   1. Stats bar (total / running / failed / completed)
 *   2. Currently running jobs
 *   3. Recent failures
 *   4. All jobs (paginated)
 *   5. YAML schedule importer
 */

import React, {
  useCallback,
  useEffect,
  useReducer,
  useRef,
} from 'react';

import {
  IDashboardStats,
  IJob,
  IJobDefinition,
  JobStatus,
  deleteJob,
  deleteJobDefinition,
  fetchDashboardStats,
  importYamlContent,
  listJobDefinitions,
  listJobs,
  stopJob,
} from './api';
import { DefinitionEditor } from './components/DefinitionEditor';
import { JobsTable } from './components/JobsTable';
import { StatusBadge } from './components/StatusBadge';

// ─── Types ────────────────────────────────────────────────────────────────────

interface IState {
  stats: IDashboardStats | null;
  allJobs: IJob[];
  jobDefinitions: IJobDefinition[];
  selectedDefinition: IJobDefinition | null;
  loading: boolean;
  error: string | null;
  filterStatus: JobStatus | 'ALL';
  yamlText: string;
  yamlResult: string | null;
  yamlLoading: boolean;
  activeTab: 'dashboard' | 'yaml';
}

type Action =
  | { type: 'LOAD_START' }
  | { type: 'LOAD_SUCCESS'; stats: IDashboardStats; jobs: IJob[]; definitions: IJobDefinition[] }
  | { type: 'LOAD_ERROR'; error: string }
  | { type: 'SET_FILTER'; status: JobStatus | 'ALL' }
  | { type: 'SET_YAML'; text: string }
  | { type: 'YAML_LOADING' }
  | { type: 'YAML_RESULT'; result: string }
  | { type: 'SET_TAB'; tab: IState['activeTab'] }
  | { type: 'DELETE_JOB'; jobId: string }
  | { type: 'DELETE_DEFINITION'; id: string }
  | { type: 'SELECT_DEFINITION'; definition: IJobDefinition | null }
  | { type: 'UPDATE_DEFINITION'; definition: IJobDefinition };

function reducer(state: IState, action: Action): IState {
  switch (action.type) {
    case 'LOAD_START':
      return { ...state, loading: true, error: null };
    case 'LOAD_SUCCESS': {
      // Keep selectedDefinition in sync with the freshly fetched list
      const refreshedSelected = state.selectedDefinition
        ? (action.definitions.find(
            d => d.job_definition_id === state.selectedDefinition!.job_definition_id
          ) ?? state.selectedDefinition)
        : null;
      return {
        ...state,
        loading: false,
        stats: action.stats,
        allJobs: action.jobs,
        jobDefinitions: action.definitions,
        selectedDefinition: refreshedSelected,
      };
    }
    case 'LOAD_ERROR':
      return { ...state, loading: false, error: action.error };
    case 'SET_FILTER':
      return { ...state, filterStatus: action.status };
    case 'SET_YAML':
      return { ...state, yamlText: action.text };
    case 'YAML_LOADING':
      return { ...state, yamlLoading: true, yamlResult: null };
    case 'YAML_RESULT':
      return { ...state, yamlLoading: false, yamlResult: action.result };
    case 'SET_TAB':
      return { ...state, activeTab: action.tab };
    case 'DELETE_JOB':
      return {
        ...state,
        allJobs: state.allJobs.filter(j => j.job_id !== action.jobId),
        stats: state.stats
          ? {
              ...state.stats,
              recent_failures: state.stats.recent_failures.filter(j => j.job_id !== action.jobId),
              in_progress: state.stats.in_progress.filter(j => j.job_id !== action.jobId),
            }
          : null,
      };
    case 'DELETE_DEFINITION':
      return {
        ...state,
        jobDefinitions: state.jobDefinitions.filter(d => d.job_definition_id !== action.id),
        selectedDefinition:
          state.selectedDefinition?.job_definition_id === action.id
            ? null
            : state.selectedDefinition,
      };
    case 'SELECT_DEFINITION':
      return { ...state, selectedDefinition: action.definition };
    case 'UPDATE_DEFINITION':
      return {
        ...state,
        selectedDefinition: action.definition,
        jobDefinitions: state.jobDefinitions.map(d =>
          d.job_definition_id === action.definition.job_definition_id ? action.definition : d
        ),
      };
    default:
      return state;
  }
}

const YAML_PLACEHOLDER = `version: "1"

schedules:
  - name: daily-report
    description: "Daily sales report"
    notebook: notebooks/my_report.py
    cron: "0 9 * * 1-5"
    timezone: "Europe/Berlin"
    output_formats:
      - html
    parameters:
      date: "\${TODAY}"
    tags:
      - daily
    enabled: true
`;

const INITIAL_STATE: IState = {
  stats: null,
  allJobs: [],
  jobDefinitions: [],
  selectedDefinition: null,
  loading: false,
  error: null,
  filterStatus: 'ALL',
  yamlText: YAML_PLACEHOLDER,
  yamlResult: null,
  yamlLoading: false,
  activeTab: 'dashboard',
};

// ─── Component ────────────────────────────────────────────────────────────────

export function Dashboard(): JSX.Element {
  const [state, dispatch] = useReducer(reducer, INITIAL_STATE);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const load = useCallback(async () => {
    dispatch({ type: 'LOAD_START' });
    try {
      const [stats, jobsResp, defsResp] = await Promise.all([
        fetchDashboardStats(),
        listJobs({ limit: 100 }),
        listJobDefinitions(),
      ]);
      dispatch({
        type: 'LOAD_SUCCESS',
        stats,
        jobs: jobsResp.jobs,
        definitions: defsResp.job_definitions,
      });
    } catch (e: unknown) {
      dispatch({ type: 'LOAD_ERROR', error: String(e) });
    }
  }, []);

  useEffect(() => {
    void load();
    intervalRef.current = setInterval(() => void load(), 15_000);
    return () => {
      if (intervalRef.current !== null) clearInterval(intervalRef.current);
    };
  }, [load]);

  const handleStop = useCallback(async (jobId: string) => {
    await stopJob(jobId);
    void load();
  }, [load]);

  const handleDelete = useCallback(async (jobId: string) => {
    if (!window.confirm('Delete this job record?')) return;
    await deleteJob(jobId);
    dispatch({ type: 'DELETE_JOB', jobId });
  }, []);

  const handleDeleteDefinition = useCallback(async (id: string) => {
    if (!window.confirm('Delete this schedule definition?')) return;
    await deleteJobDefinition(id);
    dispatch({ type: 'DELETE_DEFINITION', id });
  }, []);

  const handleYamlImport = useCallback(async () => {
    dispatch({ type: 'YAML_LOADING' });
    try {
      const result = await importYamlContent(state.yamlText);
      if (result.error) {
        dispatch({ type: 'YAML_RESULT', result: `Error: ${result.error}` });
      } else {
        dispatch({
          type: 'YAML_RESULT',
          result: `Imported ${result.imported} schedule(s).`,
        });
        void load();
      }
    } catch (e: unknown) {
      dispatch({ type: 'YAML_RESULT', result: `Error: ${String(e)}` });
    }
  }, [state.yamlText, load]);

  const definitionFilteredJobs = state.selectedDefinition
    ? state.allJobs.filter(j => j.job_definition_id === state.selectedDefinition!.job_definition_id)
    : state.allJobs;

  const filteredJobs =
    state.filterStatus === 'ALL'
      ? definitionFilteredJobs
      : definitionFilteredJobs.filter(j => j.status === state.filterStatus);

  const { stats } = state;

  return (
    <div className="marimo-scheduler-dashboard">
      {/* ── Header ── */}
      <div className="marimo-scheduler-header">
        <span className="marimo-scheduler-logo">Marimo Scheduler</span>
        <div className="marimo-scheduler-tabs">
          {(['dashboard', 'yaml'] as const).map(tab => (
            <button
              key={tab}
              className={[
                'marimo-scheduler-tab',
                state.activeTab === tab ? 'marimo-scheduler-tab--active' : '',
              ].join(' ')}
              onClick={() => dispatch({ type: 'SET_TAB', tab })}
            >
              {tab === 'dashboard' ? 'Dashboard' : 'YAML Schedules'}
            </button>
          ))}
        </div>
        <button
          className="marimo-scheduler-btn"
          onClick={() => void load()}
          disabled={state.loading}
          title="Refresh"
        >
          {state.loading ? '⟳ Refreshing…' : '⟳ Refresh'}
        </button>
      </div>

      {state.error && (
        <div className="marimo-scheduler-error">⚠ {state.error}</div>
      )}

      {/* ─────────────────── DASHBOARD TAB ─────────────────── */}
      {state.activeTab === 'dashboard' && (
        <>
          {/* Currently running */}
          {stats && stats.in_progress.length > 0 && (
            <Section title={`Running (${stats.in_progress.length})`}>
              <JobsTable
                jobs={stats.in_progress}
                onStop={jobId => void handleStop(jobId)}
                emptyMessage="No running jobs."
              />
            </Section>
          )}

          {/* Recent failures */}
          {stats && stats.recent_failures.length > 0 && (
            <Section title={`Recent Failures (${stats.recent_failures.length})`}>
              <JobsTable
                jobs={stats.recent_failures}
                onDelete={jobId => void handleDelete(jobId)}
                emptyMessage="No recent failures."
              />
            </Section>
          )}

          {/* Scheduled definitions */}
          <Section title={`Scheduled Definitions (${state.jobDefinitions.length})`}>
            <DefinitionsTable
              definitions={state.jobDefinitions}
              selectedId={state.selectedDefinition?.job_definition_id}
              onSelect={def =>
                dispatch({
                  type: 'SELECT_DEFINITION',
                  definition:
                    def.job_definition_id === state.selectedDefinition?.job_definition_id
                      ? null   // click again to deselect
                      : def,
                })
              }
              onDelete={id => void handleDeleteDefinition(id)}
            />
            {state.selectedDefinition && (
              <DefinitionEditor
                definition={state.selectedDefinition}
                onClose={() => dispatch({ type: 'SELECT_DEFINITION', definition: null })}
                onSaved={() => void load()}
              />
            )}
          </Section>

          {/* Job Runs — filtered by selected definition */}
          <Section
            title={
              state.selectedDefinition
                ? `Job Runs — ${state.selectedDefinition.name}`
                : 'Job Runs'
            }
          >
            <div className="marimo-scheduler-filter-bar">
              <label>Filter: </label>
              {(['ALL', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'QUEUED', 'STOPPED'] as const).map(
                s => (
                  <button
                    key={s}
                    className={[
                      'marimo-scheduler-filter-btn',
                      state.filterStatus === s
                        ? 'marimo-scheduler-filter-btn--active'
                        : '',
                    ].join(' ')}
                    onClick={() =>
                      dispatch({ type: 'SET_FILTER', status: s as JobStatus | 'ALL' })
                    }
                  >
                    {s === 'ALL' ? 'All' : <StatusBadge status={s} />}
                  </button>
                )
              )}
            </div>
            <JobsTable
              jobs={filteredJobs}
              onStop={jobId => void handleStop(jobId)}
              onDelete={jobId => void handleDelete(jobId)}
              emptyMessage={
                state.filterStatus === 'ALL'
                  ? 'No jobs yet. Create one with the YAML Schedules tab or the jupyter-scheduler UI.'
                  : `No ${state.filterStatus} jobs.`
              }
            />
          </Section>
        </>
      )}

      {/* ─────────────────── YAML TAB ─────────────────── */}
      {state.activeTab === 'yaml' && (
        <Section title="Import YAML Schedule Definition">
          <p className="marimo-scheduler-hint">
            Define schedules in GitHub-Actions style YAML and click{' '}
            <strong>Import</strong> to register them with the scheduler.
            You can also commit <code>*.marimo-schedule.yml</code> files to your
            workspace — they are auto-detected on startup.
          </p>
          <textarea
            className="marimo-scheduler-yaml-editor"
            value={state.yamlText}
            onChange={e => dispatch({ type: 'SET_YAML', text: e.target.value })}
            spellCheck={false}
            rows={24}
          />
          <div style={{ marginTop: 8, display: 'flex', gap: 8, alignItems: 'center' }}>
            <button
              className="marimo-scheduler-btn marimo-scheduler-btn--primary"
              onClick={() => void handleYamlImport()}
              disabled={state.yamlLoading}
            >
              {state.yamlLoading ? 'Importing…' : 'Import Schedules'}
            </button>
            {state.yamlResult && (
              <span
                className={
                  state.yamlResult.startsWith('Error')
                    ? 'marimo-scheduler-error-inline'
                    : 'marimo-scheduler-success-inline'
                }
              >
                {state.yamlResult}
              </span>
            )}
          </div>
        </Section>
      )}
    </div>
  );
}

// ─── DefinitionsTable ────────────────────────────────────────────────────────

function DefinitionsTable({
  definitions,
  selectedId,
  onSelect,
  onDelete,
}: {
  definitions: IJobDefinition[];
  selectedId?: string;
  onSelect?: (def: IJobDefinition) => void;
  onDelete?: (id: string) => void;
}): JSX.Element {
  if (definitions.length === 0) {
    return (
      <p style={{ color: 'var(--jp-ui-font-color2)', fontStyle: 'italic', padding: '8px 0' }}>
        No schedules defined yet. Use the YAML Schedules tab to import one.
      </p>
    );
  }
  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="marimo-scheduler-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Notebook</th>
            <th>Cron</th>
            <th>Timezone</th>
            <th>Active</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {definitions.map(def => (
            <tr
              key={def.job_definition_id}
              className={[
                'marimo-def-row',
                def.job_definition_id === selectedId ? 'marimo-def-row--selected' : '',
              ].join(' ')}
              onClick={() => onSelect?.(def)}
              title={def.job_definition_id === selectedId ? 'Click to deselect' : 'Click to filter runs'}
            >
              <td title={def.name}>{def.name || <em>unnamed</em>}</td>
              <td title={def.input_filename}>{def.input_filename}</td>
              <td><code>{def.schedule}</code></td>
              <td>{def.timezone || 'UTC'}</td>
              <td>{def.active ? '✓' : '—'}</td>
              <td>
                {onDelete && (
                  <button
                    className="marimo-scheduler-btn marimo-scheduler-btn--danger"
                    onClick={e => { e.stopPropagation(); onDelete(def.job_definition_id); }}
                    title="Delete this schedule"
                  >
                    Delete
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Small sub-components ────────────────────────────────────────────────────

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}): JSX.Element {
  return (
    <div className="marimo-scheduler-section">
      <h3 className="marimo-scheduler-section-title">{title}</h3>
      {children}
    </div>
  );
}

