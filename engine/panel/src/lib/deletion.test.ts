import { expect, it } from "vitest";
import { confirmDeletion, deletionTarget, settleDeletion } from "./deletion";
it("does not confirm until explicit acceptance, and supports cancellation", async () => {
  const canceled = confirmDeletion("queue production");
  expect(deletionTarget.value).toBe("queue production");
  await expect(confirmDeletion("second target")).resolves.toBe(false);
  settleDeletion(false);
  await expect(canceled).resolves.toBe(false);
  const accepted = confirmDeletion("message 123");
  settleDeletion(true);
  await expect(accepted).resolves.toBe(true);
  expect(deletionTarget.value).toBe("");
});
