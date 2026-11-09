<script setup lang="ts">
import { onMounted } from "vue";
import { TooltipProvider } from "@/components/ui/tooltip";
import AppShell from "@/components/layout/AppShell.vue";
import GateView from "@/views/GateView.vue";
import RevealView from "@/views/RevealView.vue";
import SetupView from "@/views/SetupView.vue";
import { Button } from "@/components/ui/button";
import { initAuth, session } from "@/lib/session";
import { applyTheme } from "@/lib/storage";

onMounted(() => {
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
    if (session.themeMode === "system") applyTheme("system");
  });
  void initAuth();
});
</script>

<template>
  <TooltipProvider>
    <div v-if="session.phase === 'loading'" class="flex min-h-svh items-center justify-center text-sm text-muted-foreground">
      Connecting…
    </div>
    <SetupView v-else-if="session.phase === 'setup'" />
    <div v-else-if="session.phase === 'unavailable'" class="flex min-h-svh flex-col items-center justify-center gap-3">
      <p role="alert">{{ session.gateMessage }}</p>
      <Button @click="initAuth">Retry connection</Button>
    </div>
    <RevealView v-else-if="session.phase === 'reveal'" />
    <GateView v-else-if="session.phase === 'gate'" />
    <AppShell v-else />
  </TooltipProvider>
</template>
