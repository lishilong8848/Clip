export const KNOWLEDGE_FILE_ACCEPT = '.pdf,.docx,.xlsx,.xlsm,.md,.txt,.csv,.png,.jpg,.jpeg,.webp';
const extensions = new Set(KNOWLEDGE_FILE_ACCEPT.split(','));
export const supportedKnowledgeFile = (file: File) => extensions.has(file.name.slice(file.name.lastIndexOf('.')).toLowerCase());

export function folderFile(file: File, path = file.webkitRelativePath): File {
  // Preserve folder identity without sending filesystem paths to the server.
  return path ? new File([file], path.replace(/[\\/]/g, '__'), { type: file.type, lastModified: file.lastModified }) : file;
}

export async function droppedKnowledgeFiles(entries: FileSystemEntry[], cancelled: () => boolean): Promise<File[]> {
  const files: File[] = [];
  const queue = [...entries];
  for (let cursor = 0; cursor < queue.length; cursor++) {
    if (cancelled()) return [];
    const entry = queue[cursor]!;
    if (entry.isDirectory) {
      const reader = (entry as FileSystemDirectoryEntry).createReader();
      while (!cancelled()) {
        const children = await new Promise<FileSystemEntry[]>((resolve, reject) => reader.readEntries(resolve, reject));
        if (!children.length) break;
        for (const child of children) queue.push(child);
      }
    } else if (entry.isFile) {
      const file = await new Promise<File>((resolve, reject) => (entry as FileSystemFileEntry).file(resolve, reject));
      if (supportedKnowledgeFile(file)) files.push(folderFile(file, entry.fullPath.replace(/^\//, '')));
    }
    if (cursor % 100 === 0) await new Promise(resolve => setTimeout(resolve, 0));
  }
  return files;
}

export function knowledgeUploadBatches(files: File[]): File[][] {
  const batches: File[][] = [];
  let batch: File[] = [], bytes = 0;
  for (const file of files) {
    if (batch.length && (batch.length === 10 || bytes + file.size > 300 * 1024 * 1024)) {
      batches.push(batch); batch = []; bytes = 0;
    }
    batch.push(file); bytes += file.size;
  }
  if (batch.length) batches.push(batch);
  return batches;
}
