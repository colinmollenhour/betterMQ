<script setup lang="ts">
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import AppHeader from "@/components/layout/AppHeader.vue";
import AppSidebar from "@/components/layout/AppSidebar.vue";
import { localBrokerSelected, session } from "@/lib/session";
import { useRoute } from "vue-router";
import { isStandalone } from "@/lib/config";
const route = useRoute();
</script>

<template>
  <SidebarProvider>
    <AppSidebar />
    <SidebarInset>
      <Alert v-if="session.splitAuth" variant="destructive" class="m-2">
        <AlertTitle>Split auth across brokers</AlertTitle>
        <AlertDescription>
          This API URL hits nodes with different local auth. Use one broker for dev, or complete
          panel setup on each cluster node.
        </AlertDescription>
      </Alert>
      <AppHeader />
      <div class="flex-1 overflow-auto p-4">
        <p class="mb-3 text-xs text-muted-foreground">Local API target: {{ session.apiBase }}</p>
        <Alert v-if="!localBrokerSelected() && !isStandalone() && !['/infra', '/docs'].includes(route.path)">
          <AlertTitle>Remote cell selected</AlertTitle>
          <AlertDescription>
            This screen operates only on the local broker ({{ session.apiBase }}).
            Select its local cell to use it, or open the selected cell's embedded panel.
            Infrastructure supports federated inspection.
          </AlertDescription>
        </Alert>
        <router-view v-else />
      </div>
    </SidebarInset>
  </SidebarProvider>
</template>
