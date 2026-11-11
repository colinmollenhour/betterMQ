import { expect, it } from "vitest";
import { defaultRetry, mergeRetry } from "./payload";

it("serializes zero retries instead of inheriting the broker default", () => {
  const payload = JSON.parse(JSON.stringify(mergeRetry({}, defaultRetry())));
  expect(payload.max_retries ?? 3).toBe(0);
  expect(payload.retry_backoff).toBeUndefined();
});

it("keeps explicit positive retry policies", () => {
  expect(mergeRetry({}, { ...defaultRetry(), maxRetries: 2 })).toMatchObject({
    max_retries: 2, retry_backoff: { kind: "exponential", initialMs: 500 },
  });
});
