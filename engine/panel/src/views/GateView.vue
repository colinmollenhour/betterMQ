<script setup lang="ts">
import { ref } from "vue";
import AuthLayout from "@/components/layout/AuthLayout.vue";
import Field from "@/components/form/Field.vue";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { isStandalone } from "@/lib/config";
import { errText } from "@/lib/payload";
import { regenerateToken, session, setToken, startApp, submitGateToken, validateSessionToken } from "@/lib/session";

const token = ref(session.token);
const password = ref("");
const error = ref(session.gateMessage);
const showRegen = ref(false);
const busy = ref(false);

async function continueWithToken() {
  error.value = "";
  busy.value = true;
  try {
    submitGateToken(token.value);
    await validateSessionToken();
    startApp();
  } catch (e) {
    setToken("");
    error.value = errText(e);
  } finally {
    busy.value = false;
  }
}

async function regen() {
  error.value = "";
  busy.value = true;
  try {
    await regenerateToken(password.value);
  } catch (e) {
    error.value = errText(e);
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <AuthLayout>
    <div class="space-y-4">
      <div>
        <h1 class="text-lg font-semibold">API token</h1>
        <p v-if="isStandalone()" class="mt-1 text-sm text-muted-foreground">
          Enter the standalone server's admin bearer credential (BETTERMQ_CLUSTER_SECRET).
        </p>
        <p v-else class="mt-1 text-sm text-muted-foreground">
          Paste your <code class="font-mono">sk_local_…</code> token, or regenerate one with your panel password.
        </p>
      </div>
      <Field label="API token">
        <Input
          v-model="token"
          type="password"
          class="font-mono"
          :placeholder="isStandalone() ? 'Admin credential' : 'sk_local_…'"
          autocomplete="off"
          @keydown.enter="continueWithToken"
        />
      </Field>
      <p v-if="error" class="text-sm text-destructive">{{ error }}</p>
      <Button class="w-full" :disabled="busy" @click="continueWithToken">Continue</Button>
      <Button v-if="!isStandalone()" class="w-full" variant="ghost" @click="showRegen = !showRegen">Open Settings</Button>
      <div v-if="showRegen" class="space-y-3 border-t pt-4">
        <Field label="Panel password">
          <Input v-model="password" type="password" autocomplete="current-password" />
        </Field>
        <Button class="w-full" variant="outline" :disabled="busy" @click="regen">Regenerate token</Button>
      </div>
    </div>
  </AuthLayout>
</template>
