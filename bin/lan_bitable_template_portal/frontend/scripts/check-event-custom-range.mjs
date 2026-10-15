import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { build } from 'esbuild';
import { parse, compileScript } from '@vue/compiler-sfc';
import { adaptEventPeriodPage } from './workspace-event-adapter.mjs';

const bundle = await build({entryPoints:['src/workspace-v2/eventPeriod.ts'],bundle:true,write:false,format:'esm'});
const {eventPeriodRange:range,occurrenceTimestamp,mergeEventMonths,eventTrend}=await import('data:text/javascript;base64,'+Buffer.from(bundle.outputFiles[0].text).toString('base64'));
const now=Date.parse('2026-10-15T02:00:00Z');
const custom=(start,end)=>range('custom','',now,[start,end]);
assert.deepEqual(custom('2026-01-15','2026-03-15').months,['2026-01','2026-02','2026-03']);
assert.deepEqual(custom('2025-12-31','2026-02-01').months,['2025-12','2026-01','2026-02']);
assert.equal(custom('2024-02-29','2024-02-29').end-custom('2024-02-29','2024-02-29').start,86400000);
for(const dates of [['2026-02-29','2026-03-01'],['2026-02-01','2026-01-01'],['','2026-01-01'],['2026-01-01','2028-01-01']]) {
  const invalid=custom(...dates);assert(invalid.error);assert.deepEqual(invalid.months,[],'invalid ranges never issue monthly reads');
}
const r=custom('2026-01-15','2026-03-15');
const item=(record_id,occurrence_time)=>({record_id,occurrence_time});
const data=mergeEventMonths([{records:[item('before','2026-01-14 23:59:59'),item('first','2026-01-15 00:00:00')]},
  {records:[item('middle','2026-02-10 09:30:00'),item('middle','2026-02-10 09:30:00')]},
  {records:[item('last','2026-03-15 23:59:59'),item('after','2026-03-16 00:00:00')]}],r);
assert.deepEqual(data.records.map(row=>row.record_id),['last','middle','first']);
assert.equal(r.start,Date.parse('2026-01-14T16:00:00Z'));
const year=custom('2026-01-01','2026-12-31'), bins=eventTrend(data.records,year);
assert(bins.length<=62,'long-range charts stay bounded');
assert.equal(bins.reduce((sum,bin)=>sum+bin.value,0),3);
assert.equal(bins[0].start,year.start);assert.equal(bins.at(-1).end,year.end);
assert.equal(range('week','',Date.parse('2026-10-01T02:00:00Z')).caption,'2026.09.28 - 2026.10.04');
assert.equal(range('history','2024-02',now).months[0],'2024-02');
assert.equal(occurrenceTimestamp('2026-01-15T00:00:00+08:00'),r.start);
const source=await readFile('src/components/EventManagementPage.vue','utf8');
const adapted=adaptEventPeriodPage(source);
compileScript(parse(adapted).descriptor,{id:'event-custom'});
assert.match(adapted,/v-model:dates="eventDates"/);
assert.match(adapted,/offset\+=3/);
assert.match(adapted,/if\(range.error\)throw/);
assert.match(adapted,/event_start/);
assert.doesNotMatch(source,/eventDates|EventPeriodFilter/,'legacy event page is untouched');
console.log('PASS: custom ranges, all intermediate months, Beijing day boundaries, validation, bounded charts and V2 adapter.');
