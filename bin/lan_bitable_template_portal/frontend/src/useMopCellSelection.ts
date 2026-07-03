import { ref } from "vue";
import {
  makeMopCellKey,
  mopCellOverlayStyle,
  parseMopCellKey,
  parseMopCellKeyList,
  type MopCellOverlayMode,
  type MopCellPosition,
} from "./mopSheetUtils";

export function useMopCellSelection() {
  const activeKey = ref("");
  const selectedKeys = ref<string[]>([]);
  const anchor = ref<MopCellPosition | null>(null);
  const selecting = ref(false);
  const dragging = ref(false);
  const overlayStyle = ref<Record<string, string>>({});
  const sheetScrollRef = ref<HTMLElement | null>(null);
  let startPoint: { x: number; y: number } | null = null;

  function keyFor(sheetName: string, rowIndex: number, colIndex: number): string {
    return makeMopCellKey(sheetName, rowIndex, colIndex);
  }

  function activePosition(): MopCellPosition | null {
    return activeKey.value ? parseMopCellKey(activeKey.value) : null;
  }

  function selectedPositions(): MopCellPosition[] {
    return parseMopCellKeyList(selectedKeys.value);
  }

  function clear(): void {
    activeKey.value = "";
    selectedKeys.value = [];
    selecting.value = false;
    dragging.value = false;
    anchor.value = null;
    startPoint = null;
    overlayStyle.value = {};
  }

  function finish(): void {
    selecting.value = false;
    dragging.value = false;
    anchor.value = null;
    startPoint = null;
  }

  function isSelected(key: string): boolean {
    return selectedKeys.value.includes(key);
  }

  function setSingle(key: string, rowIndex: number, colIndex: number): void {
    activeKey.value = key;
    selectedKeys.value = [key];
    anchor.value = { row: rowIndex, col: colIndex };
  }

  function start(rowIndex: number, colIndex: number, event: MouseEvent, key: string): void {
    setSingle(key, rowIndex, colIndex);
    selecting.value = true;
    dragging.value = false;
    startPoint = { x: event.clientX, y: event.clientY };
  }

  function setRange(input: {
    sheetName: string;
    toRow: number;
    toCol: number;
    columns: number[];
    visible: (row: number, col: number) => boolean;
  }): void {
    if (!anchor.value) return;
    const minRow = Math.min(anchor.value.row, input.toRow);
    const maxRow = Math.max(anchor.value.row, input.toRow);
    const minCol = Math.min(anchor.value.col, input.toCol);
    const maxCol = Math.max(anchor.value.col, input.toCol);
    const keys: string[] = [];
    for (let row = minRow; row <= maxRow; row += 1) {
      for (const col of input.columns) {
        if (col < minCol || col > maxCol) continue;
        if (!input.visible(row, col)) continue;
        keys.push(keyFor(input.sheetName, row, col));
      }
    }
    selectedKeys.value = keys.length
      ? keys
      : [keyFor(input.sheetName, anchor.value.row, anchor.value.col)];
  }

  function extend(input: {
    row: number;
    col: number;
    event: MouseEvent;
    sheetName: string;
    columns: number[];
    visible: (row: number, col: number) => boolean;
  }): boolean {
    if (!selecting.value || !anchor.value) return false;
    if (!input.visible(input.row, input.col)) return false;
    if (!dragging.value) {
      const moved = startPoint ? Math.hypot(input.event.clientX - startPoint.x, input.event.clientY - startPoint.y) : 0;
      const changedCell = input.row !== anchor.value.row || input.col !== anchor.value.col;
      if (!changedCell || moved < 8) return false;
      dragging.value = true;
    }
    setRange({
      sheetName: input.sheetName,
      toRow: input.row,
      toCol: input.col,
      columns: input.columns,
      visible: input.visible,
    });
    return true;
  }

  function activeElement(): HTMLElement | null {
    if (!activeKey.value || !sheetScrollRef.value) return null;
    return sheetScrollRef.value.querySelector(`[data-mop-cell-key="${activeKey.value}"]`) as HTMLElement | null;
  }

  function updateOverlay(mode: MopCellOverlayMode): void {
    const cell = activeElement();
    if (!cell) {
      overlayStyle.value = {};
      return;
    }
    const rect = cell.getBoundingClientRect();
    overlayStyle.value = mopCellOverlayStyle(rect, mode, window.innerWidth, window.innerHeight);
  }

  return {
    activeKey,
    selectedKeys,
    overlayStyle,
    sheetScrollRef,
    activePosition,
    selectedPositions,
    clear,
    finish,
    isSelected,
    setSingle,
    start,
    extend,
    updateOverlay,
  };
}
