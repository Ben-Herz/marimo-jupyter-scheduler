import React from 'react';
import { JobStatus } from '../api';

interface IProps {
  status: JobStatus;
}

const STATUS_META: Record<JobStatus, { label: string; modifier: string }> = {
  COMPLETED:   { label: 'Completed',   modifier: 'completed' },
  FAILED:      { label: 'Failed',      modifier: 'failed' },
  IN_PROGRESS: { label: 'Running',     modifier: 'running' },
  QUEUED:      { label: 'Queued',      modifier: 'queued' },
  STOPPED:     { label: 'Stopped',     modifier: 'stopped' },
  PAUSED:      { label: 'Paused',      modifier: 'paused' },
};

export function StatusBadge({ status }: IProps): JSX.Element {
  const meta = STATUS_META[status] ?? { label: status, modifier: 'stopped' };
  return (
    <span className={`jp-StatusBadge jp-StatusBadge--${meta.modifier}`}>
      {meta.label}
    </span>
  );
}
