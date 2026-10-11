"""Upload an actual 300 MiB fixture batch through the isolated knowledge route."""
import asyncio
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
from fastapi import FastAPI
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
from openclaw_service.assistant.lighthouse_knowledge_routes import install_knowledge_routes
from check_knowledge_local_runtime import peak_memory


class NoModel:
    def close(self):
        pass


async def main():
    with tempfile.TemporaryDirectory(prefix='clipflow-kb-stream-') as directory:
        root = Path(directory)
        source = root / 'fixture.txt'
        with source.open('wb') as stream:
            stream.truncate(100 * 1024**2)
        kb = KnowledgeBase(root / 'knowledge', embedder=NoModel(), start_worker=False)
        app = FastAPI()
        async def authorize(_request):
            return {'id': 'isolated-stream-fixture', 'name': '测试', 'is_admin': True}
        install_knowledge_routes(app, authorize, lambda: kb)
        try:
            started = time.perf_counter()
            with ExitStack() as handles:
                files = [('files', (f'fixture-{i}.txt', handles.enter_context(source.open('rb')), 'text/plain')) for i in range(3)]
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://isolated') as client:
                    response = await client.post('/api/assistant/knowledge/files', files=files)
            assert response.status_code == 200, response.text
            result = response.json()['data']
            assert not result['errors'] and len(result['items']) == 3
            assert all(item['size'] == 100 * 1024**2 for item in result['items'])
            assert len(list((kb.root / 'originals').rglob('*.txt'))) == 1, 'identical content must not duplicate physical files'
            assert not list((kb.root / 'uploads').iterdir()), 'no abandoned staging files'
            print(json.dumps({'ok': True, 'files': 3, 'each_mib': 100, 'batch_mib': 300,
                              'elapsed_seconds': round(time.perf_counter() - started, 2),
                              'process_peak_working_set_mib': peak_memory(),
                              'scope': 'actual streaming multipart parsing and storage; no document indexing or live business data'}))
        finally:
            kb.close()


if __name__ == '__main__':
    asyncio.run(main())
