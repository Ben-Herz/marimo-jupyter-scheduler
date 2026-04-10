import React, { useCallback, useState } from 'react';
import { IJob } from '../api';
import { StatusBadge } from './StatusBadge';

interface IProps {
  jobs: IJob[];
  onStop?: (jobId: string) => void;
  onDelete?: (jobId: string) => void;
  emptyMessage?: string;
}

function formatTime(iso?: string): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString(undefined, {
      dateStyle: 'short',
      timeStyle: 'medium',
    });
  } catch {
    return iso;
  }
}

function shortenPath(p: string): string {
  const parts = p.split('/');
  return parts.length > 2 ? `…/${parts.slice(-2).join('/')}` : p;
}

export function JobsTable({
  jobs,
  onStop,
  onDelete,
  emptyMessage = 'No jobs found.',
}: IProps): JSX.Element {
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const handleStop = useCallback(
    (jobId: string) => onStop?.(jobId),
    [onStop]
  );

  const handleDelete = useCallback(
    (jobId: string) => onDelete?.(jobId),
    [onDelete]
  );

  if (jobs.length === 0) {
    return (
      <p style={{ color: 'var(--jp-ui-font-color2)', fontStyle: 'italic', padding: '8px 0' }}>
        {emptyMessage}
      </p>
    );
  }

  const colSpan = 6;

  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="marimo-scheduler-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Notebook</th>
            <th>Status</th>
            <th>Started</th>
            <th>Finished</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map(job => {
            const hasMessage = !!job.status_message;
            const isExpanded = expandedId === job.job_id;
            const isClickable = hasMessage;

            return (
              <React.Fragment key={job.job_id}>
                <tr
                  className={isClickable ? 'marimo-job-row--clickable' : undefined}
                  onClick={isClickable ? () => setExpandedId(isExpanded ? null : job.job_id) : undefined}
                  title={isClickable ? (isExpanded ? 'Click to collapse' : 'Click to see error') : undefined}
                >
                  <td title={job.name}>{job.name || <em>unnamed</em>}</td>
                  <td title={job.input_filename}>{shortenPath(job.input_filename)}</td>
                  <td>
                    <StatusBadge status={job.status} />
                    {hasMessage && (
                      <span
                        style={{ marginLeft: 5, fontSize: 11, color: 'var(--jp-ui-font-color2)', userSelect: 'none' }}
                        aria-label="Has error details"
                      >
                        {isExpanded ? '▲' : '▼'}
                      </span>
                    )}
                  </td>
                  <td>{formatTime(job.start_time)}</td>
                  <td>{formatTime(job.end_time)}</td>
                  <td>
                    <div style={{ display: 'flex', gap: 4 }} onClick={e => e.stopPropagation()}>
                      {job.status === 'IN_PROGRESS' && onStop && (
                        <button
                          className="marimo-scheduler-btn marimo-scheduler-btn--warn"
                          onClick={() => handleStop(job.job_id)}
                          title="Stop this job"
                        >
                          Stop
                        </button>
                      )}
                      {onDelete && (
                        <button
                          className="marimo-scheduler-btn marimo-scheduler-btn--danger"
                          onClick={() => handleDelete(job.job_id)}
                          title="Delete this job record"
                        >
                          Delete
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
                {isExpanded && hasMessage && (
                  <tr className="marimo-job-row--error">
                    <td colSpan={colSpan}>
                      <pre className="marimo-run-status-pre">{job.status_message}</pre>
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
