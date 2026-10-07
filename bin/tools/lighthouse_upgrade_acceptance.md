# Lighthouse upgrade acceptance

The independent resident-backend objective has a separate current ledger in
lighthouse_resident_acceptance.md. The historical checked items below do not
claim completion of that larger objective.

This ledger records the OpenClaw migration and PC conversation acceptance.
Business-cloud writes were tested only with isolated data, not production records.

## Runtime and integration

- [x] Pinned project-owned Windows runtime installs and starts without modifying global Node.
- [x] Authenticated Gateway protocol streams, cancels, reconnects and resumes the exact owned run.
- [x] Per-account Gateway state, model credentials and file access are isolated.
- [x] A typed business plugin uses the original portal authorization and prepared operations.
- [x] Every configured provider has text, streaming, tool and image capability evidence.
- [x] Automatic context compaction and model switching retain permitted context only.
- [x] Real public search returns sources and timestamps without exporting business data.

## Business and security

- [x] Capability matrix covers every requested module and native permission source.
- [x] Event/notice/repair counts use the established definitions and full authorized data.
- [x] Empty, incomplete, failed and forbidden queries remain distinct.
- [x] Query refinements retain scope, time and status constraints.
- [x] Business forms reuse original fields, attachments, confirmations and operation IDs.
- [x] Confirmation, restart, cancellation and retries cannot repeat completed business writes.
- [x] Raw Bitable writes, work-order execution and protected account/signature administration are denied.
- [x] Cross-account, cross-building, file and credential leakage tests pass.

## PC UI and distribution

- [x] 620x700 bounded default panel, expand/restore, readable theme and compact settings.
- [x] Streaming, supplementation, stopping, retry, Markdown, tables, code, files and sources work.
- [x] Long conversations preserve reading position; persistent avatar/drawers do not obstruct controls.
- [x] Account appearance, drag, return position and 200px sizing survive navigation and reload.
- [x] Legacy conversation archive migration and model context import are permission-aware.
- [x] Portable/patch inclusion, dependency integrity and runtime-data exclusion are verified.
- [x] Automated regression tests, live model checks and PC browser acceptance pass.
- [x] Original business workflow regression passes using isolated records.

OpenClaw is now the default. The previous engine is retained for one migration
generation only and requires LIGHTHOUSE_AGENT_ENGINE=legacy explicitly.

## Evidence 2026-10-03

- OpenClaw 2026.8.1, Node 24.16.0, Gateway v4; actual trimmed archive launch and typed plugin loop passed.
- 12 dependency parts (161.9 MiB) published only to tblUECEItaUpOWvd; actual authenticated download, part/whole hash and atomic install passed.
- Four configured WanWu providers: text/stream/tool passed. MiniMax/GLM/Qwen image passed; Deepseek image unsupported, uses OCR fallback.
- Final 1264 isolated assistant tests passed, two opt-in public network tests skipped; public search/weather were separately checked live.
- PC stream test: send clears input, supplement narrows to D, original run resumes without repost, stop/continue, expand/restore, Markdown table, no page errors.
- Repeated PC drag test: three toggle/drag cycles, no old/new coordinate flash, independent panel/icon positions, pointercancel cleanup passed.
- Learning is query-only under original learning_scopes; all mutations use the original page.
- Reviewed SKILL.md catalog and typed plugin extension entry added; no terminal or arbitrary plugin installation enabled.
- Native context harness: 17 passes; separate roots and cross-account history rejection, bounded idle eviction/restart, scope narrowing, genuine compaction metadata and retained context.
- The post-switch same request contains the old context marker, new model name, new endpoint path and exact new synthetic auth header.
- Native browser: actual public message route -> Gateway -> typed plugin -> authorized original-shaped repair query -> rendered table/source. Reload resumes the same run; stop/retry and cross-account stream rejection pass.
- Native business proposal remains unwritten until original PortalAgent confirmation; repeated confirmation after worker recreation writes once. Real Gateway socket disconnect recovers the exact run without a second agent RPC.
- A synthetic provider 401 echoing the key stays private; no plaintext key is found in owned runtime files, including failure logs.
- Full package preflight passed: 53 notice identity, 1372 learning/assistant, 32 convergence, 123 notice/repair/process, 33 critical guard and 18 patch transport tests. The final extra weather-context test is included in the separate 1264 assistant run.
- Frontend typecheck and final production build passed; native, streaming and drag PC browser scripts were rerun on that build.
- All 15 required runtime-source/plugin/skill files are included. bin/runtime, account data, model keys and development-agent folders are excluded.
- Weather follow-ups reuse only the preceding public user question's city; internal history is rejected. Search query time is not claimed as news publication time.

## Follow-up audit 2026-10-04

- Natural follow-ups retain the business subject, time and status; new model/general questions do not inherit unfinished-work queries. Month-only dates, calendar-day dates, recent periods and remaining-count follow-ups have deterministic regressions.
- Current repair-only queries no longer load unrelated notice/plan groups. Plans still load ongoing notices to exclude already-started sources.
- Usage guidance does not require a live business-count query. Actual counts/status queries still require current evidence.
- A real default-engine probe reproduced seven failures caused by treating separate native assistant messages as a single append-only text stream. The adapter now respects commentary, item boundaries, explicit replacements and authoritative snapshots without item IDs; exact-run/sequence checks remain.
- The real configured provider with synthetic APIs passed all 12 module/general scenarios, then an additional eight provider scenarios. Fixture schema/authentication defects were returned to DeepCode and independently corrected before acceptance.
- Native proposal, human confirmation, repeated confirmation, actual socket recovery and provider-credential-echo tests passed; no production business records were written.
- An intermittent preflight failure exposed phone-like digits inside a 24-character cabinet row hash. Strict row/event identifiers are now preserved only in machine-ID fields; free-text PII and secret fields remain filtered.
- PC checks on the final dist: bounded 620x700 panel, 760px expansion, long-input growth, immediate composer clearing, folded processing, stop/restart, model settings, reopen position and no console errors. Screenshots are in output/playwright/assistant-audit; their displayed totals are synthetic.
- Frontend typecheck/build and final package preflight passed: 53 notice identity, 1456 learning/assistant, 32 convergence, 123 notice/repair/process, 33 critical guard and 18 transport checks. The two opt-in public checks were also run separately and passed (including honest unavailable handling).
- Five previously omitted assistant regression modules are now included in package preflight. This audit did not publish a new ordinary patch or change Qt event sending, work-order execution, permissions or real business totals.

## Startup follow-up 2026-10-04

- Run 5c9d4d4d failed after about 80 seconds in Gateway startup, before inference. The old process was only started on the first model question and its 75-second deadline was too tight for cold initialization.
- Actual isolated startup with all canonical typed tools took 75.83 seconds before reducing unused startup services. Follow-up runs took 55.31 and 58.39 seconds; same-process warm reuse took 0.033-0.034 seconds. These are local measurements, not a startup SLA.
- Portal startup prepares dependencies in a background worker. Authenticated page loading warms only the current account's configured model; missing/disabled profiles do not spawn a process. Credentials and business callbacks stay account-scoped.
- Warmup uses the exact real-turn tool configuration, submits no model message, reads no business records/files and registers no active business callback. OpenClaw channel, canvas and startup-model warming are disabled; original portal message sending is unchanged.
- Readiness now checks the local read-only models.list RPC behind the same startup gate as agent, not just an authenticated socket. A bounded 180-second provisioning budget is separate from model inference. Readiness failures carry runtime_startup diagnostics with safe stage/timing logs.
- First messages join in-flight preparation. Owned warm tasks/processes are cancelled and closed on shutdown, with the original process cap, low Windows process priority and Job Object cleanup retained.
- Independently reviewed/corrected DeepCode's startup fixtures; 21 startup regressions pass with asynchronous teardown and bounded waits. The final package preflight passes 1736 tests (two optional public-network checks skipped), including notice/repair/lifecycle and patch integrity checks.
- A final actual Gateway/health probe with the startup gate passed, and the configured model passed ordinary chat plus synthetic repair-followup querying. All owned probe processes exited; no real business records or ordinary patch publication were performed.

## Delivery and limits

- The fixed runtime mirror is published and an actual download/install was verified. The ordinary patch was not published to Gitee during this acceptance run.
- Run package_portable.py and require both publication and download verification to succeed before notifying users. Portal startup prepares/downloads the fixed 161.9 MiB runtime when absent; per-account Gateway warmup requires an authenticated account with a configured model.
- Offline fallback folder: build_output/mirror_install/lighthouse_openclaw. Its parent location on a user's machine must be bin/runtime.
- Missing/unsupported vision capability uses reliable OCR instead of inventing image content. Configured provider capability results are local, not a guarantee for arbitrary new endpoints or credentials.
- Public services have no availability SLA. Unavailable or unverified data is reported as unknown, not fabricated.
- Native permissions and original workflows are reused; cloud-side schema/permissions and live business totals were not changed or certified by synthetic tests.
- Reviewed plugins/tools/skills can be extended through the documented development process. Chat-driven shell access and unreviewed plugin installation remain disabled.
