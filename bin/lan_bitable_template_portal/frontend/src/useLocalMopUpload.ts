import { ref } from "vue";

const LOCAL_MOP_MAX_BYTES = 20 * 1024 * 1024;

export function useLocalMopUpload() {
  const busy = ref(false);
  const status = ref("");
  const message = ref("");

  function reset(): void {
    status.value = "";
    message.value = "";
  }

  function fail(text: string): void {
    status.value = "failed";
    message.value = text || "请上传 Excel MOP 文件。";
  }

  function begin(): void {
    busy.value = true;
    status.value = "";
    message.value = "上传中 / 识别中";
  }

  function finish(): void {
    busy.value = false;
  }

  function succeed(fileName: string, warnings: unknown[] = []): void {
    status.value = "success";
    const warningText = warnings.length ? `；${String(warnings[0] || "")}` : "";
    message.value = `已选中：${fileName}${warningText}`;
  }

  function failFromError(error: unknown, fallback = "本地 MOP 上传失败"): string {
    const text = error instanceof Error ? error.message : fallback;
    fail(text);
    return text;
  }

  function validateFile(file: File): string {
    if (!/\.(xlsx|xlsm|xls)$/i.test(file.name || "")) {
      return "请选择 xlsx、xlsm 或 xls 格式的 Excel 文件。";
    }
    if (file.size > LOCAL_MOP_MAX_BYTES) {
      return "MOP 文件超过 20MB，请压缩或更换文件后再上传。";
    }
    return "";
  }

  return {
    busy,
    status,
    message,
    reset,
    fail,
    begin,
    finish,
    succeed,
    failFromError,
    validateFile,
  };
}
