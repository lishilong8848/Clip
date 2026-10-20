# Assistant Workspace Acceptance

## Shared Behavior

- Web and Feishu use the same assistant tools and original authorization.
- Native notice/forms/jobs/confirmations are not replaced by workspace documents.
- Complex public questions can choose research automatically; `/深入研究` remains optional.
- Research checkpoints retain successful reads. Recovery is explicitly resumed in the original personal conversation or topic and never replays a business write.
- Documents support exact targeted replacement, revisions, version diff and restore. Conflicts preserve user edits. Plaintext documents export as private .txt files; native business files remain unchanged.
- Packages hold authorized attachment references, documents and clearly marked historical query excerpts. They are not a new live business database.
- Long-term memory requires a direct user instruction or an explicit UI confirmation. Users can read/edit/delete it. Roles, current statuses and credentials are not memories.
- Speech usage is available to all authenticated assistant users. Only provider configuration requires administration. Playback defaults off; receiving a reply does not start audio.
- Voice transcripts enter the original sender/topic conversation, with its ordinary validation and confirmation requirements.
- Long screenshots can be cropped/tiled and OCR'd; pixel comparison does not claim semantic understanding. Vision input still requires the configured model capability.
- Of the four configured WanWu models, only MiniMax can receive images after explicit opt-in. GLM/Qwen/Deepseek receive OCR text only, including during fallback and when older settings incorrectly enabled images.
- Outbound chat requests bound the whole input plus reserved output before contacting the provider. Output uses one `max_tokens` field capped at 5000. The SDK retains its earlier default 64000 compaction threshold; the four WanWu providers are bounded by the reported 149952 hard ceiling, with any smaller explicit limit honored. Other models default to 64000. Budget estimates are conservative, not vendor-tokenizer measurements. Irreducible current input is rejected without truncating it or deleting history.
- Chat endpoints cannot be replaced with embeddings endpoints. Company embeddings continue to use the existing local BGE engine.

## External Prerequisites

- Speech requires separately configured HTTPS transcription/synthesis services and explicit allowed origins. Text-model credentials are never assumed to provide speech.
- Sandbox Python requires an already-installed local Linux Docker/Podman engine with enforced seccomp/cgroups quotas and a trusted pinned image in `LIGHTHOUSE_SANDBOX_IMAGE`. No runtime or image is installed automatically.
- Sandbox has no host mounts or network and never falls back to host Python. Container security still depends on the engine/kernel/image being patched and trusted.
- Video understanding and image generation are explicitly unavailable until an actual service is integrated.

## Verification

Backend isolated regressions:

```text
python -m unittest bin.test_lighthouse_workspace bin.test_lighthouse_workspace_integration bin.test_lighthouse_research bin.test_lighthouse_sandbox bin.test_lighthouse_visual bin.test_lighthouse_media_bridge bin.test_lighthouse_voice
```

Frontend fixture (no production data):

```text
npm run check:assistant-workspace
```

Release checks:

```text
python package_portable.py --preflight-test
```

These tests do not establish live provider quality, real Feishu delivery, physical iOS microphone/playback compatibility or actual container-engine availability. They do not publish updates or send real business messages.
