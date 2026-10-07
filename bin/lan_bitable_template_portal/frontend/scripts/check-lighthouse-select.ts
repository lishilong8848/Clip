import assert from 'node:assert/strict';
import { labelledOptions, selectedLabel, selectedValue, smallSingleChoice } from '../src/lighthouseSelect.ts';

const options = [{ value: 'record-one', label: 'Same title' }, { value: 'record-two', label: 'Same title' }];
const labelled = labelledOptions(options);
assert.notEqual(labelled[0].label, labelled[1].label);
assert.equal(selectedValue(options, labelled[1].label), 'record-two');
assert.equal(selectedLabel(options, 'record-one'), labelled[0].label);
assert.equal(selectedValue(options, 'missing'), undefined);
assert.equal(selectedValue(options, ''), '');
assert.equal(selectedLabel(options, ''), '');
assert.equal(selectedLabel(options, 'deleted-id'), '已选择');
assert.equal(selectedValue([{ value: 0, label: 'Zero' }], 'Zero'), 0);
assert.equal(selectedValue([{ value: false, label: 'False' }], 'False'), false);
assert.equal(smallSingleChoice(options), true);
assert.equal(smallSingleChoice([{ value: 1, disabled: true }]), false);
assert.equal(smallSingleChoice(Array.from({ length: 5 }, (_, value) => ({ value }))), false);
console.log('Lighthouse select checks passed.');
