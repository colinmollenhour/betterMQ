<script setup lang="ts">
import { ref } from "vue";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
const model = defineModel<string>({ required: true });
const revealed = ref(false);
</script>

<template>
  <div class="flex gap-1">
    <Input v-model="model" :type="revealed ? 'text' : 'password'" autocomplete="off" class="font-mono" placeholder="Blank = generate a unique secret" />
    <Button type="button" variant="outline" size="sm" :aria-pressed="revealed" @click="revealed = !revealed">{{ revealed ? "Hide" : "Reveal" }}</Button>
  </div>
</template>
