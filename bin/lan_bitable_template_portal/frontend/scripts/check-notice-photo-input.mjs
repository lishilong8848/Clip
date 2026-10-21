import assert from 'node:assert/strict';
import {noticePhotoFiles} from '../src/noticePhotoInput.ts';

const image = new File(['fixture'], 'photo.png', {type: 'image/png'});
const text = new File(['fixture'], 'notes.txt', {type: 'text/plain'});
function event(type, files) {
  const value = new Event(type, {cancelable: true, bubbles: true});
  Object.defineProperty(value, type === 'paste' ? 'clipboardData' : 'dataTransfer', {value: {files}});
  return value;
}
for (const type of ['paste', 'drop']) {
  const mixed = event(type, [text, image]);
  assert.deepEqual(noticePhotoFiles(mixed), [image]);
  assert(mixed.defaultPrevented); assert(mixed.cancelBubble);
  assert.deepEqual(noticePhotoFiles(mixed), [], 'same event is handled once');
  assert.deepEqual(noticePhotoFiles(event(type, [image])), [image], 'a new explicit event remains retryable');
  const unsupported = event(type, [text]);
  assert.deepEqual(noticePhotoFiles(unsupported), []); assert(unsupported.cancelBubble);
  assert.equal(unsupported.defaultPrevented, type === 'drop', 'ignore unsupported files without blocking normal paste');
  const plain = event(type, []);
  assert.deepEqual(noticePhotoFiles(plain), []); assert(!plain.defaultPrevented); assert(!plain.cancelBubble);
}
console.log('PASS: image-only transfer, event deduplication, explicit retry, unsupported files and normal text paste/drop');
