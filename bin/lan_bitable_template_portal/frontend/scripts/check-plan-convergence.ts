import assert from 'node:assert/strict';
import { allDraftItems, draftsSubmitKey, restoreDrafts, roomNodeKey, roomToSelParam, submittedKey, selectionItemCount, type Draft } from '../src/planConvergenceRules.ts';

assert.equal(selectionItemCount(100, 200, 100000, 50000), 5000000000);
assert.equal(selectionItemCount(2, 3, 0, 0), 6);
assert.equal(selectionItemCount(2, 3, 4, 5), 20);
assert.equal(selectionItemCount(2, 3, 0, 5), 15);
assert.equal(selectionItemCount(2, 0, 0, 5), 5);

const normal: Draft[] = [{label: 'A', rule_type: 'normal', items: [{scope_type: 'zone', zone: 'Z1'}]}];
const common: Draft[] = [{...normal[0], rule_type: 'common'}];
assert.notEqual(draftsSubmitKey(normal), draftsSubmitKey(common));
assert.notEqual(draftsSubmitKey(normal), draftsSubmitKey([{...normal[0], label: 'B'}]));
const groups: Draft[] = [...normal, {label: 'B', rule_type: 'common', items: [{scope_type: 'floor', floor: 'F1'}]}];
assert.notEqual(draftsSubmitKey(groups), draftsSubmitKey([...groups].reverse()));
const items = allDraftItems(groups);
assert.equal(submittedKey(items), submittedKey([...items].reverse()));
assert.equal(items[1].rule_group_no, 2);
assert.equal(restoreDrafts(items)[1].rule_type, 'common');
assert.deepEqual(roomToSelParam({level: 'room', key: 'R1', name: 'R1', devices: 1, _checked: true,
  zone: 'Z1', building: 'B1', floor: 'F1'}), {zone: 'Z1', building: 'B1', floor: 'F1', room: 'R1'});
const room = {level: 'room' as const, key: 'R1', name: 'R1', devices: 1, _checked: true,
  zone: 'Z1', building: 'B1', floor: 'F1'};
assert.notEqual(roomNodeKey(room), roomNodeKey({...room, building: 'B2'}));
const sameName: Draft[] = [{ label: 'same names', rule_type: 'normal', items: [{ scope_type: 'device', inst_name: '设备', ins_id: 'one' }, { scope_type: 'device', inst_name: '设备', ins_id: 'two' }] }];
assert.equal(allDraftItems(sameName).length, 2);
assert.deepEqual(restoreDrafts(allDraftItems(sameName))[0].items.map(item => item.ins_id), ['one', 'two']);
console.log('[PlanConvergenceCheck] OK');
