// @vitest-environment happy-dom
import { createApp, nextTick, reactive } from "vue";
import { afterEach, expect, it, vi } from "vitest";
import { confirmDeletion, settleDeletion } from "@/lib/deletion";

vi.mock("vue-router", () => ({ useRoute: () => ({ fullPath: "/queues" }) }));
vi.mock("@/lib/session", () => ({ session: reactive({ selectedCellId: "local", apiBase: "http://broker.test" }) }));
import DeleteConfirmation from "./DeleteConfirmation.vue";

afterEach(() => { settleDeletion(false); document.body.innerHTML = ""; });

it("accepts the Delete button before the dialog close event can cancel it", async () => {
  const root = document.createElement("div");
  document.body.append(root);
  const app = createApp(DeleteConfirmation);
  app.mount(root);
  const confirmation = confirmDeletion("queue production");
  await nextTick();
  await nextTick();
  const button = [...document.querySelectorAll("button")].find((b) => b.textContent === "Delete");
  expect(button).toBeDefined();
  button!.click();
  await expect(confirmation).resolves.toBe(true);
  app.unmount();
});
