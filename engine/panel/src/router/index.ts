import { createRouter, createWebHistory } from "vue-router";
import { isStandalone } from "@/lib/config";
import { ref } from "vue";
import { recoverChunk } from "@/lib/chunkRecovery";

export const navigationError = ref("");

export const router = createRouter({
  history: createWebHistory("/panel/"),
  routes: [
    { path: "/", name: "overview", component: () => import("@/views/OverviewView.vue") },
    { path: "/infra", name: "infra", component: () => import("@/views/InfraView.vue") },
    { path: "/queues", name: "queues", component: () => import("@/views/QueuesView.vue") },
    { path: "/groups", name: "groups", component: () => import("@/views/GroupsView.vue") },
    { path: "/publish", name: "publish", component: () => import("@/views/PublishView.vue") },
    { path: "/enqueue", name: "enqueue", component: () => import("@/views/EnqueueView.vue") },
    { path: "/flows", name: "flows", component: () => import("@/views/FlowsView.vue") },
    { path: "/schedules", name: "schedules", component: () => import("@/views/SchedulesView.vue") },
    { path: "/dlq", name: "dlq", component: () => import("@/views/DlqView.vue") },
    { path: "/http", name: "http", component: () => import("@/views/HttpView.vue") },
    { path: "/docs", name: "docs", component: () => import("@/views/DocsView.vue") },
    { path: "/settings", name: "settings", component: () => import("@/views/SettingsView.vue") },
    { path: "/:pathMatch(.*)*", redirect: "/" },
  ],
});

router.beforeEach((to) => {
  if (isStandalone() && !["/infra", "/docs"].includes(to.path)) return "/infra";
});

router.onError((error, to) => {
  let recovered = false;
  try {
    recovered = recoverChunk(error, sessionStorage, () => location.assign(router.resolve(to).href));
  } catch { /* Storage may be unavailable. */ }
  if (!recovered) navigationError.value = "Navigation failed after an update or connection error. Reload the panel to try again.";
});
