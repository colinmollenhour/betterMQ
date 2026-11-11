const RELOAD_KEY = "bettermq:chunk-reload-attempted";

export function recoverChunk(error: unknown, storage: Pick<Storage, "getItem" | "setItem">, reload: () => void): boolean {
  const message = error instanceof Error ? error.message : String(error);
  if (!/failed to fetch dynamically imported module|importing a module script failed|loading (?:css )?chunk|unable to preload css/i.test(message)) return false;
  try {
    if (storage.getItem(RELOAD_KEY)) return false;
    storage.setItem(RELOAD_KEY, "1");
  } catch {
    return false; // No reliable loop guard: let the operator reload manually.
  }
  reload();
  return true;
}
