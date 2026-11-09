import { reactive } from "vue";
import { apiGet, publicUrl } from "@/lib/api";

export type QueueRow = {
  queue_id?: string;
  id?: string;
  queue?: string;
  name?: string;
  url?: string;
  parallelism?: number;
  max_retries?: number;
};
export type GroupRow = { group_id?: string; id?: string; name?: string };
export type FlowRow = { flow_id?: string; id?: string; key?: string; parallelism?: number; rate?: number; period_secs?: number };

export const catalog = reactive({
  queues: [] as QueueRow[],
  groups: [] as GroupRow[],
  flows: [] as FlowRow[],
  errors: { queues: "", groups: "", flows: "" },
  loaded: { queues: false, groups: false, flows: false },
});

export function queueId(q: QueueRow): string {
  return q.queue_id || q.id || "";
}

export function groupId(g: GroupRow): string {
  return g.group_id || g.id || "";
}

export function flowId(f: FlowRow): string {
  return f.flow_id || f.id || "";
}

export async function refreshQueues() {
  try {
    const d = await apiGet<{ queues?: QueueRow[] }>(publicUrl("/v1/queues"));
    catalog.queues = d.queues || [];
    catalog.errors.queues = "";
    catalog.loaded.queues = true;
  } catch (error) {
    catalog.errors.queues = String(error);
    throw error;
  }
}

export async function refreshGroups() {
  try {
    const d = await apiGet<{ groups?: GroupRow[] }>(publicUrl("/v1/groups"));
    catalog.groups = d.groups || [];
    catalog.errors.groups = "";
    catalog.loaded.groups = true;
  } catch (error) {
    catalog.errors.groups = String(error);
    throw error;
  }
}

export async function refreshFlows() {
  try {
    const d = await apiGet<{ flows?: FlowRow[] }>(publicUrl("/v1/flows"));
    catalog.flows = d.flows || [];
    catalog.errors.flows = "";
    catalog.loaded.flows = true;
  } catch (error) {
    catalog.errors.flows = String(error);
    throw error;
  }
}
