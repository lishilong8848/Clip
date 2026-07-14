export const REPAIR_STATUS_INVALIDATED_EVENT = "clipflow:repair-status-invalidated";

export function invalidateRepairStatus(): void {
  window.dispatchEvent(new Event(REPAIR_STATUS_INVALIDATED_EVENT));
}
