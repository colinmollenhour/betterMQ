import { ref } from "vue";

export const deletionTarget = ref("");
let resolvePending: ((confirmed: boolean) => void) | null = null;

export function confirmDeletion(target: string): Promise<boolean> {
  if (resolvePending) return Promise.resolve(false);
  deletionTarget.value = target;
  return new Promise((resolve) => { resolvePending = resolve; });
}

export function settleDeletion(confirmed: boolean) {
  const resolve = resolvePending;
  resolvePending = null;
  deletionTarget.value = "";
  resolve?.(confirmed);
}
