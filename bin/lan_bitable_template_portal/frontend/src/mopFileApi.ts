import { requestJson, type Dict } from "./api/client";

export function fetchEngineerMopBootstrap(scope: string): Promise<Dict> {
  return requestJson(`/api/engineer/mop/bootstrap?scope=${encodeURIComponent(scope)}`);
}

export function bindEngineerMop(payload: Dict): Promise<Dict> {
  return requestJson("/api/engineer/mop/bind", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function previewEngineerMop(params: {
  scope: string;
  mopRecordId: string;
  fileToken: string;
  fileName: string;
}): Promise<Dict> {
  const query = new URLSearchParams({
    scope: params.scope,
    mop_record_id: params.mopRecordId,
    file_token: params.fileToken,
    file_name: params.fileName,
  });
  return requestJson(`/api/engineer/mop/preview?${query.toString()}`);
}

export function fillEngineerMop(payload: Dict): Promise<Dict> {
  return requestJson("/api/engineer/mop/fill", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function uploadSignedEngineerMop(payload: Dict): Promise<Dict> {
  return requestJson("/api/engineer/mop/upload-signed", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function resetEngineerMop(payload: {
  scope: string;
  filledFilePath: string;
  mopRecordId: string;
  fileToken: string;
  fileName: string;
}): Promise<Dict> {
  return requestJson("/api/engineer/mop/reset", {
    method: "POST",
    body: JSON.stringify({
      scope: payload.scope,
      filled_file_path: payload.filledFilePath,
      mop_record_id: payload.mopRecordId,
      file_token: payload.fileToken,
      file_name: payload.fileName,
    }),
  });
}
