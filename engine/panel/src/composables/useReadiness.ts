import { computed } from "vue";
import { useTimestamp } from "@vueuse/core";
import { session } from "@/lib/session";
import { readinessLabel } from "@/lib/health";

export function useReadiness() {
  const now = useTimestamp({ interval: 1000 });
  return computed(() => readinessLabel(session.ready, session.readyAt, now.value));
}
