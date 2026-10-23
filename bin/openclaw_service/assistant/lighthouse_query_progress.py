"""Read-only coverage and bounded progress for the existing model tool loop."""
import hashlib
import json

from .lighthouse_ai import safe_data
from .lighthouse_queries import query_result_state


def query_progress(sources, queries):
    latest = {}
    for source in sources:
        latest[source.get('query_key') or (source.get('url'), source.get('title'))] = source
    receipts = []
    for source in latest.values():
        data = queries.get(source.get('query_ref'), source.get('data'))
        state, guidance = query_result_state({'ok': source.get('available', True), 'data': data})
        receipt = {key: source.get(key) for key in ('query_ref', 'title', 'query_conditions', 'queried_at')}
        receipt.update(state=state, guidance=guidance)
        if isinstance(data, dict):
            receipt['coverage'] = safe_data({key: data[key] for key in (
                'complete', 'list_complete', 'total', 'loaded_count', 'has_more', 'next_cursor',
                'next_offset', 'page', 'page_size', 'query_completeness') if key in data})
        receipts.append(receipt)
    return {'sources': receipts, 'gaps': [row for row in receipts if row['state'] in {
        'partial', 'unavailable', 'empty_page'}], 'no_source': not receipts,
        'note': 'Coverage describes returned sources, not proof that every user goal is answered.'}


class ReadRepairBudget:
    """More evidence earns further read repair; repeating the same snapshot does not."""
    MAX_ROUNDS = 4

    def __init__(self):
        self.rounds, self.unchanged = 0, 0
        self.seen = set()

    def allow(self, sources, queries):
        signatures = set()
        for source in sources:
            data = queries.get(source.get('query_ref'), source.get('data'))
            if isinstance(data, dict):
                data = {key: value for key, value in data.items()
                        if key not in {'queried_at', 'fetched_at', 'cached_at', 'request_id', 'request_logid'}}
            signatures.add(hashlib.sha256(json.dumps([
                source.get('query_key') or [source.get('url'), source.get('query_conditions')],
                source.get('available', True), data], sort_keys=True, ensure_ascii=False).encode()).hexdigest())
        progress = bool(signatures - self.seen)
        self.seen.update(signatures)
        self.unchanged = 0 if progress or self.rounds == 0 else self.unchanged + 1
        if self.rounds >= self.MAX_ROUNDS or self.rounds and self.unchanged >= 2:
            return False
        self.rounds += 1
        return True
