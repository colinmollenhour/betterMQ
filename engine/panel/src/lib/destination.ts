export function prepareDestination(form: { url: string; secret: string }): string | null {
  try {
    const url = new URL(form.url.trim());
    if (!["http:", "https:"].includes(url.protocol)) return "Destination must be an HTTP(S) URL";
  } catch {
    return "Enter a destination URL";
  }
  if (!form.secret.trim()) {
    try {
      const bytes = crypto.getRandomValues(new Uint8Array(32));
      form.secret = `whsec_${Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("")}`;
    } catch {
      return "Secure secret generation is unavailable; enter your own secret";
    }
  }
  return null;
}
