export function cellCredentialError(raw: string, editing: boolean): string | null {
  const token = raw.trim();
  if (!token && editing) return null;
  return token.startsWith("sk_local_") && token.length > "sk_local_".length
    ? null : "Enter the target cell's sk_local_ admin token (not the cluster secret).";
}
