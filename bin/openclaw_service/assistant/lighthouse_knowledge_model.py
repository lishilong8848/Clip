"""Pinned, offline BGE assets. No company documents or user configuration."""
import hashlib
from pathlib import Path
import shutil

REVISION = '46fbe35fd4374a00fee7de77dfddaeb6dd6a2c59'
BUNDLE = Path('bin/resources/knowledge_model/bge-small-zh-v1.5')
FILES = {
    'config.json': '9088751d39abbf86ec3d19ffca92ad62ad19075f7e59712e6c71217fa125d1d3',
    'model_optimized.onnx': '1294ea4b6331115a353d81f96b85e8c8d7fdcc284453d5b2fab5b016230aad38',
    'special_tokens_map.json': 'b6d346be366a7d1d48332dbc9fdf3bf8960b5d879522b7799ddba59e76237ee3',
    'tokenizer.json': '48cea5d44424912a6fd1ea647bf4fe50b55ab8b1e5879c3275f80e339e8fae26',
    'tokenizer_config.json': 'e6f3b96db926a37d4039995fbf5ad17de158dfb8f6343d607e4dbaad18d75f5a',
}


def verify_model(folder):
    folder = Path(folder)
    for name, expected in FILES.items():
        with (folder / name).open('rb') as stream:
            actual = hashlib.file_digest(stream, 'sha256').hexdigest()
        if actual != expected:
            raise ValueError(f'知识库模型文件校验失败：{name}，请重新更新程序。')
    return folder


def bundle_model(project, destination):
    project, destination = Path(project), Path(destination)
    if not (project / 'bin/openclaw_service/assistant/lighthouse_knowledge_local.py').is_file():
        return []
    source = project / BUNDLE
    if not source.is_dir():
        source = (project / 'bin/data/lighthouse_openclaw/knowledge_base/models'
                  / 'models--Qdrant--bge-small-zh-v1.5/snapshots' / REVISION)
    verify_model(source)
    target = destination / BUNDLE
    target.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        shutil.copy2(source / name, target / name)
    return [BUNDLE / name for name in FILES]
