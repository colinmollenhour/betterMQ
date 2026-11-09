<script setup lang="ts">
import { onUnmounted, watch } from "vue";
import { useRoute } from "vue-router";
import { AlertDialog, AlertDialogCancel, AlertDialogContent, AlertDialogDescription,
  AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { deletionTarget, settleDeletion } from "@/lib/deletion";
import { session } from "@/lib/session";
const route = useRoute();
watch(() => [route.fullPath, session.selectedCellId], () => settleDeletion(false));
onUnmounted(() => settleDeletion(false));
</script>

<template>
  <AlertDialog :open="Boolean(deletionTarget)" @update:open="(open) => { if (!open) settleDeletion(false); }">
    <AlertDialogContent>
      <AlertDialogHeader>
        <AlertDialogTitle>Confirm deletion</AlertDialogTitle>
        <AlertDialogDescription>
          Delete {{ deletionTarget }} on {{ session.apiBase }}? This cannot be undone.
        </AlertDialogDescription>
      </AlertDialogHeader>
      <AlertDialogFooter>
        <AlertDialogCancel @click="settleDeletion(false)">Cancel</AlertDialogCancel>
        <Button variant="destructive" @click="settleDeletion(true)">Delete</Button>
      </AlertDialogFooter>
    </AlertDialogContent>
  </AlertDialog>
</template>
