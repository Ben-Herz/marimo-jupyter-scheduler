/**
 * API client for marimo-jupyter-scheduler backend endpoints.
 *
 * Also re-exports helpers for calling the core jupyter-scheduler REST API,
 * since our dashboard reads job data from both places.
 */

import { ServerConnection } from '@jupyterlab/services';
import { URLExt } from '@jupyterlab/coreutils';

// ─── Types ────────────────────────────────────────────────────────────────────

export type JobStatus =
  | 'QUEUED'
  | 'IN_PROGRESS'
  | 'COMPLETED'
  | 'FAILED'
  | 'STOPPED'
  | 'PAUSED';

export interface IJob {
  job_id: string;
  job_definition_id?: string;
  name: string;
  input_filename: string;
  status: JobStatus;
  status_message?: string;
  start_time?: string;
  end_time?: string;
  output_formats?: string[];
  parameters?: Record<string, unknown>;
  tags?: string[];
}

export interface IJobDefinition {
  job_definition_id: string;
  name: string;
  input_filename: string;
  schedule: string;
  timezone?: string;
  output_formats?: string[];
  parameters?: Record<string, unknown>;
  tags?: string[];
  active: boolean;
}

export interface IDashboardStats {
  total: number;
  by_status: Partial<Record<JobStatus, number>>;
  recent_failures: IJob[];
  in_progress: IJob[];
  warning?: string;
}

export interface IYamlImportResult {
  imported: number;
  jobs: Array<Record<string, unknown>>;
  error?: string;
}

// ─── Base request helper ──────────────────────────────────────────────────────

async function requestJSON<T>(
  path: string,
  init: RequestInit = {}
): Promise<T> {
  const settings = ServerConnection.makeSettings();
  const url = URLExt.join(settings.baseUrl, path);

  const response = await ServerConnection.makeRequest(url, init, settings);
  const text = await response.text();

  if (!response.ok) {
    let message = `HTTP ${response.status} ${response.statusText}`;
    try {
      const json = JSON.parse(text);
      message = json.message ?? json.error ?? message;
    } catch {
      // ignore parse error
    }
    throw new ServerConnection.ResponseError(response, message);
  }

  if (!text) return undefined as unknown as T;
  return JSON.parse(text) as T;
}

// ─── Marimo-scheduler custom endpoints ───────────────────────────────────────

export async function fetchDashboardStats(): Promise<IDashboardStats> {
  return requestJSON<IDashboardStats>('/marimo-scheduler/api/v1/dashboard');
}

export async function importYamlContent(content: string): Promise<IYamlImportResult> {
  return requestJSON<IYamlImportResult>('/marimo-scheduler/api/v1/yaml-import', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ content }),
  });
}

export async function fetchYamlFiles(): Promise<{ files: string[] }> {
  return requestJSON<{ files: string[] }>('/marimo-scheduler/api/v1/yaml-files');
}

// ─── jupyter-scheduler core API ──────────────────────────────────────────────

export interface IJobListResponse {
  jobs: IJob[];
  total_count: number;
}

export interface IJobDefinitionListResponse {
  job_definitions: IJobDefinition[];
  total_count: number;
}

export async function listJobs(params: {
  status?: JobStatus;
  job_definition_id?: string;
  limit?: number;
  offset?: number;
} = {}): Promise<IJobListResponse> {
  const qs = new URLSearchParams();
  if (params.status) qs.set('status', params.status);
  if (params.job_definition_id) qs.set('job_definition_id', params.job_definition_id);
  if (params.limit !== undefined) qs.set('max_items', String(params.limit));
  if (params.offset !== undefined) qs.set('next_token', String(params.offset));
  const query = qs.toString() ? `?${qs.toString()}` : '';
  return requestJSON<IJobListResponse>(`/scheduler/jobs${query}`);
}

export async function getJob(jobId: string): Promise<IJob> {
  return requestJSON<IJob>(`/scheduler/jobs/${jobId}`);
}

export async function listJobDefinitions(): Promise<IJobDefinitionListResponse> {
  return requestJSON<IJobDefinitionListResponse>('/scheduler/job_definitions');
}

export async function stopJob(jobId: string): Promise<void> {
  await requestJSON<unknown>(`/scheduler/jobs/${jobId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: 'STOPPED' }),
  });
}

export async function deleteJob(jobId: string): Promise<void> {
  await requestJSON<unknown>(`/scheduler/jobs/${jobId}`, {
    method: 'DELETE',
  });
}

export async function createJobDefinition(
  def: Omit<IJobDefinition, 'job_definition_id'>
): Promise<IJobDefinition> {
  return requestJSON<IJobDefinition>('/scheduler/job_definitions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(def),
  });
}

export async function deleteJobDefinition(id: string): Promise<void> {
  await requestJSON<unknown>(`/scheduler/job_definitions/${id}`, {
    method: 'DELETE',
  });
}

export async function updateJobDefinition(
  id: string,
  patch: Partial<{
    name: string;
    input_filename: string;
    schedule: string;
    timezone: string;
    output_formats: string[];
    parameters: Record<string, string>;
    tags: string[];
    active: boolean;
  }>
): Promise<void> {
  await requestJSON<unknown>(`/scheduler/job_definitions/${id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(patch),
  });
}
