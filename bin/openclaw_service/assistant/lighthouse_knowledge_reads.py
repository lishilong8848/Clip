"""Turn-local, text-only document reads within the answer's shared budget."""
import asyncio
import copy
import time

from .lighthouse_ai import AssistantError


class DocumentReads:
    def __init__(self):
        self.receipts = {}
        self.cache = {}
        self.pending = {}
        self.retried = set()

    @property
    def incomplete(self):
        return any(row['status'] != 'read' or row['text_limited'] for row in self.receipts.values())

    @property
    def retry_pending(self):
        return any(key not in self.retried for key in self.pending)

    async def expand(self, knowledge, actor, items, *, profile, model, read, authorize, deadline, budget,
                     retry_failed=False):
        from .lighthouse_knowledge_evidence import _current, MAX_ITEMS, MAX_TEXT

        groups = {}
        for item in items[:MAX_ITEMS]:
            groups.setdefault((item['document_id'], item['version']), []).append(item)
        # Failed locators survive independently of usable evidence. Never send
        # their old excerpts to synthesis, and keep the existing per-pass hit cap.
        slots = MAX_ITEMS - sum(len(group) for group in groups.values())
        for key, locators in self.pending.items():
            if not retry_failed or key in self.retried:
                continue
            for locator in locators:
                group = groups.get(key, [])
                if slots and all(row['ordinal'] != locator['ordinal'] for row in group):
                    groups.setdefault(key, []).append(copy.deepcopy(locator))
                    slots -= 1
        selected = {}
        for key, group in groups.items():
            if key in self.pending and not (retry_failed and key not in self.retried):
                # A retry cap applies to unread sections, not successfully
                # recovered evidence that can still pass the current-version check.
                cached_ordinals = {row['ordinal'] for row in self.cache.get(key, {}).get('items', [])}
                group = [row for row in group if row['ordinal'] in cached_ordinals]
            if group:
                selected[key] = group
        groups = selected
        stop = min(deadline, time.monotonic() + budget)
        result, remaining = [], MAX_TEXT
        for index, (key, candidates) in enumerate(groups.items()):
            # Account authorization errors must never become document read failures.
            current = await authorize()
            if (current['id'] != actor['id']
                    or 'modules' in current and 'knowledge' not in current['modules']):
                raise AssistantError('登录身份或知识库权限已变化，请重新提问。', 403)
            started = time.monotonic()
            allowance = max(0, stop - started) / (len(groups) - index)
            document_deadline = started + allowance
            receipt = {'document_id': key[0], 'version': key[1], 'status': 'not_attempted',
                       'requested_ordinals': [row['ordinal'] for row in candidates],
                       'returned_ordinals': [], 'scope': 'matched_sections_and_neighbors',
                       'document_complete': False, 'visual_interpretation': False,
                       'current_version_checked': False, 'access_checked': False,
                       'text_limited': False, 'text_characters': 0,
                       'attempts': self.receipts.get(key, {}).get('attempts', 0),
                       'budget_ms': int(allowance * 1000), 'elapsed_ms': 0}
            self.receipts[key] = receipt
            if allowance <= 0 or remaining <= 0:
                self.pending.setdefault(key, self._locators(candidates))
                continue
            if key in self.pending:
                self.retried.add(key)
            receipt['attempts'] += 1
            try:
                cached = self.cache.get(key)
                if (cached and cached['actor'] == current and cached['profile'] == profile
                        and cached['ordinals'] == receipt['requested_ordinals']):
                    # _current checks ACL, active/external version and stored hash;
                    # it does not open raw files, expand context or interpret images.
                    await read(_current, knowledge, current, copy.deepcopy([*candidates, *cached['items']]), profile,
                               _budget=allowance)
                    expanded = copy.deepcopy(cached['items'])
                else:
                    # Timeout cannot stop a worker thread. Give it private inputs so
                    # a late return cannot mutate evidence retained by this answer.
                    inputs = copy.deepcopy(candidates)
                    for row in inputs:
                        row.pop('_evidence_receipt', None)
                        row.pop('read_receipt', None)
                        row['visual_evidence'] = []
                    found = await read(knowledge.answer_evidence, copy.deepcopy(current), inputs,
                                       profile=copy.deepcopy(profile), model=model,
                                       include_visual=False, _budget=allowance)
                    expanded = found['items']
                expected = {(row['document_id'], row['version'], row['ordinal']) for row in candidates}
                identities = [(row['document_id'], row['version'], row['ordinal']) for row in expanded]
                if (not expanded or len(set(identities)) != len(identities) or any(
                        (row['document_id'], row['version'], row['ordinal']) not in expected
                        or not isinstance(row.get('text'), str)
                        for row in expanded)):
                    raise ValueError('Invalid text evidence')
                latest = await authorize()
                if (latest['id'] != actor['id']
                        or 'modules' in latest and 'knowledge' not in latest['modules']):
                    raise AssistantError('登录身份或知识库权限已变化，请重新提问。', 403)
                # The existing reader validates source hashes and external versions.
                # Refresh ACLs too if the actor's grants changed during the read.
                if latest != current:
                    if time.monotonic() >= document_deadline:
                        raise asyncio.TimeoutError
                    await read(_current, knowledge, latest, expanded, profile,
                               _budget=document_deadline - time.monotonic())
                self.cache[key] = {'actor': copy.deepcopy(latest), 'profile': copy.deepcopy(profile),
                                   'ordinals': receipt['requested_ordinals'], 'items': copy.deepcopy(expanded)}
                returned = []
                for position, row in enumerate(expanded):
                    item = copy.deepcopy(row)
                    # Calls to the existing reader each have their own text budget;
                    # retain its original aggregate ceiling across all documents.
                    later = sum(len(group) for group in list(groups.values())[index + 1:])
                    reserve = min(600 * (later + len(expanded) - position - 1), remaining)
                    limit = max(0, remaining - reserve)
                    if not limit:
                        receipt['text_limited'] = True
                        continue
                    if len(item['text']) > limit:
                        item['text'] = item['text'][:limit]
                        item['coverage'] = {**item.get('coverage', {}), 'complete': False,
                                            'original_document_complete': False,
                                            'missing': list(dict.fromkeys([
                                                *item.get('coverage', {}).get('missing', []), 'text_budget']))}
                        item['context'] = [{**part, 'truncated': True} for part in item.get('context', [])]
                    # Image metadata must not veto useful text. Strip observations
                    # from the answer packet without parsing or understanding images.
                    # Adding coverage metadata invalidates a signed item; version-only
                    # reuse above uses _current, never the raw-file expansion path.
                    item.pop('_evidence_receipt', None)
                    item['visual_evidence'] = []
                    remaining -= len(item['text'])
                    receipt['text_limited'] |= bool(set(item.get('coverage', {}).get('missing', []))
                        & {'text_budget', 'expansion_budget', 'original_text_unavailable'})
                    returned.append(item)
                receipt.update(status='read' if returned else 'unavailable',
                               current_version_checked=True, access_checked=True,
                               returned_ordinals=[row['ordinal'] for row in returned],
                               read_ordinals=sorted({ordinal for row in returned
                                   for ordinal in row.get('coverage', {}).get('ordinals', [row['ordinal']])}),
                               text_characters=sum(len(row['text']) for row in returned))
                receipt['text_limited'] |= len(returned) != len(candidates)
                result.extend(returned)
            except asyncio.TimeoutError:
                receipt['status'] = 'timeout'
            except AssistantError as exc:
                if exc.status in {401, 403}:
                    raise
                receipt['status'] = 'not_current' if exc.status in {404, 409, 410} else 'unavailable'
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                receipt['status'] = 'unavailable'
            receipt['elapsed_ms'] = int((time.monotonic() - started) * 1000)
            pending = {row['ordinal']: row for row in self.pending.get(key, [])}
            pending.update({row['ordinal']: row for row in self._locators(candidates)})
            if receipt['status'] == 'read':
                for ordinal in receipt['returned_ordinals']:
                    pending.pop(ordinal, None)
                if pending:
                    self.pending[key] = list(pending.values())
                    receipt['text_limited'] = True
                else:
                    self.pending.pop(key, None)
            elif receipt['status'] in {'timeout', 'unavailable'}:
                self.pending[key] = list(pending.values())
            else:
                self.pending.pop(key, None)
            receipt['pending_ordinals'] = sorted(row['ordinal'] for row in self.pending.get(key, []))
            for row in result:
                if (row['document_id'], row['version']) == key:
                    row['read_receipt'] = copy.deepcopy(receipt)
        return result

    @staticmethod
    def _locators(candidates):
        return [{key: copy.deepcopy(row[key]) for key in (
            'document_id', 'version', 'ordinal', 'name', 'location', 'url',
            'source_version', 'source_hash', 'source_created_at', 'source_kind') if key in row}
            for row in candidates]
