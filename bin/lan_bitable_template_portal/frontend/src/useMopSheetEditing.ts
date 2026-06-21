import { ref } from "vue";
import type { LooseDict } from "./types";

export function useMopSheetEditing() {
  const checkboxStates = ref<Record<string, string>>({});
  const maintenanceValues = ref<Record<string, string>>({});
  const cellEdits = ref<Record<string, string>>({});
  const clipboardCellText = ref("");
  const filledResult = ref<LooseDict | null>(null);
  const uploadedAt = ref("");

  function clearOutputState(): void {
    filledResult.value = null;
    uploadedAt.value = "";
  }

  function resetSheetValues(): void {
    checkboxStates.value = {};
    maintenanceValues.value = {};
    cellEdits.value = {};
    clipboardCellText.value = "";
    clearOutputState();
  }

  function setCheckbox(key: string, value: string): void {
    checkboxStates.value = {
      ...checkboxStates.value,
      [key]: value,
    };
    clearOutputState();
  }

  function setMaintenanceValue(key: string, value: string): void {
    maintenanceValues.value = {
      ...maintenanceValues.value,
      [key]: value,
    };
    clearOutputState();
  }

  function setCellEdit(key: string, value: string): void {
    cellEdits.value = {
      ...cellEdits.value,
      [key]: value,
    };
    clearOutputState();
  }

  function clearCellEdit(key: string): void {
    const next = { ...cellEdits.value };
    delete next[key];
    cellEdits.value = next;
    clearOutputState();
  }

  function setClipboardText(value: string): void {
    clipboardCellText.value = String(value || "");
  }

  function replaceCellEdits(next: Record<string, string>): void {
    cellEdits.value = { ...next };
    clearOutputState();
  }

  function replaceCheckboxStates(next: Record<string, string>): void {
    checkboxStates.value = { ...next };
    clearOutputState();
  }

  function replaceSheetValues(next: {
    checkboxStates?: Record<string, string>;
    maintenanceValues?: Record<string, string>;
    cellEdits?: Record<string, string>;
  }): void {
    if (next.checkboxStates) checkboxStates.value = { ...next.checkboxStates };
    if (next.maintenanceValues) maintenanceValues.value = { ...next.maintenanceValues };
    if (next.cellEdits) cellEdits.value = { ...next.cellEdits };
    clearOutputState();
  }

  return {
    checkboxStates,
    maintenanceValues,
    cellEdits,
    clipboardCellText,
    filledResult,
    uploadedAt,
    clearOutputState,
    resetSheetValues,
    setCheckbox,
    setMaintenanceValue,
    setCellEdit,
    clearCellEdit,
    setClipboardText,
    replaceCellEdits,
    replaceCheckboxStates,
    replaceSheetValues,
  };
}
