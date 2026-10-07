# Unified ClipFlow assistant startup

Current objective (2026-10-06): use only `启动程序.bat` or
`bin/refactored_main.py`. The portal starts a hidden, below-normal-priority
assistant worker in the background; shutdown stops the worker and its shared gateway.
There is no separate public OpenClaw Python/BAT entry or scheduled-task launcher.
The existing private store, account isolation, business bridge and permissions
remain unchanged. Runtime preparation never blocks Qt or portal readiness.
One loopback Node gateway and port serve up to twenty concurrent accounts;
agents, sessions, models, credentials and tool authority remain account-private.

Current checks: `test_openclaw_python_entry`, `test_openclaw_service_launcher`,
`test_openclaw_service_client`, `test_openclaw_service_update`,
`test_openclaw_portal_startup`, `test_openclaw_packaging_imports`, and
`tools/check_openclaw_windows_lifecycle.py`. The native lifecycle probe replaces
the obsolete detached recovery, private-console, and updater-start probes.

## Current Skills And Chat Acceptance

### Latest Repair Consistency And Complete Regression Gate (2026-10-06)

- The offline audit rebuilt the repair index only in its protected private
  database copy. All 450 projects and 787 followups agreed with independent
  native grouping, detail progress and sixteen scope/state pagination checks.
  The original database stayed read-only and all networking/DNS was blocked.
- One active project lacks building data. Pending-work aggregation now matches
  repair overview: it warns rather than guesses a building or certifies an
  incomplete total. Valid rows remain usable. The real-copy assistant check
  returned six verified scoped projects and an explicit incomplete-count warning
  in 0.019 s; index/detail verification took 1.307 s. These are local projection
  measurements, not end-to-end production or cloud freshness guarantees.
- A timeout regression exposed an unobserved shared-read failure after its
  consumer timed out. Cleanup now gathers both completed and cancelled tasks;
  existing limits, cancellation and business retry behavior remain unchanged.
  The final pending/scope/query subset passed 91 tests without that task error.
- Production-dist Key browser checks passed again with synthetic credentials:
  no stored Key in inputs, password masking, prevented copy/cut/drag/context
  menu, replacement paste allowed, controls locked during save and Key cleared
  afterward. No actual model settings or business records were changed.
- On the final runtime tree, `package_portable.py --preflight-test` exited zero:
  118 files compiled; readiness/static checks; 53 notice identity, 1769
  learning/assistant, 190 backend/update, 35 convergence, 123 notice/repair/
  lifecycle, 33 guard and 18 transport tests. Of 2221 tests, 2219 passed and
  two opt-in public-network checks were skipped. Publishing fixtures used a
  temporary file:// repository, never the real update remote.

Current requested inspection acceptance:

| Requirement | Evidence |
| --- | --- |
| Business routing, distinct counts and original permission boundaries | Native eighteen-case model check; current query/scope/business regression gate |
| Fast useful local reads without fabricated current-cloud totals | Real-copy projections, repair index/detail audit and cached event provenance |
| General chat and basic public tools | Nineteen-case configured-model check, capability/citation/public-read regressions |
| Twenty private accounts on one gateway | Native twenty-account concurrency/context/model/tool/restart gate |
| Saved model Key cannot be retrieved or copied from settings | Current production-dist browser check and account-model/bridge tests |
| No VPN dependency for startup or other business pages | Current convergence tests: offline opening/local rules, concurrent unrelated request, retained cache/auth |
| Delivery-tree regression and original business workflows | Current complete preflight, exit zero; no subsequent runtime edit |

The requested code inspection and isolated acceptance are complete. Cold SDK
startup can still take tens of seconds in the background; normal portal use is
not gated on it. Upstream availability and universal model factual correctness
are not guaranteed. No real VPN or business-table write test was performed, no
production service was restarted and no user patch was published. Earlier
sections retain the evidence and limitations of their respective iterations.

### Latest Real Local Read And Key Protection Evidence (2026-10-06)

- Read-only inspection found the event table last fully refreshed at 11:12,
  while current pending answers displayed only the new query time. Event
  projection now carries its last cloud-sync timestamp through the private
  bridge's JSON list response. Pending replies, including zero results, identify
  counts as known local records, note the Qt unfinished overlay and explicitly
  state that this turn did not resynchronize cloud data. No new cloud read or
  rejected useful nonempty cache was introduced.
- `tools/audit_lighthouse_local_data.py` makes a consistent private SQLite
  backup using mode=ro/query_only on the original, protects only the temporary
  directory, forbids networking/DNS and reuses native projection methods on
  the copy. Native schema checks/migrations are never bypassed. The backup took
  4.528 s; all eleven snapshot header counts matched their 43477 stored rows.
  Twenty-one events/orders/MOP projections across seven scopes passed stable-ID
  uniqueness and scope containment. Event reads took 0.185-0.284 s; other local
  projections took 0.009-0.019 s. The private copy was removed after the check.
- Production-dist Key browser verification passed again: saved credentials
  absent from the input, password masking, blocked copy/cut/context-menu/drag,
  allowed replacement paste, disabled controls during save and cleared input
  afterward. Only synthetic credentials and mocked local APIs were used.
- A red check reproduced loss of three-part metadata across JSON bridging and
  the missing origin time. The final pending/query/stream/scope/account group
  passed 140 tests; proxy/pending/plan-I/O/query coverage passed 124 tests.
  Actual stored snapshot counts are not certified as current cloud totals;
  cold startup and other real-data integration checks remain open. No original
  database mutation, real provider request or business-table write occurred.

### Latest General Conversation And Business Query Evidence (2026-10-06)

- A red regression reproduced the false classification of ALPHA-PUBLIC-47 /
  BETA-PUBLIC-82 as a building reference. Only hyphenated building codes now
  require the same preceding boundary as scope extraction; real building/room
  references and all original permissions are retained. Chat-label changes no
  longer trigger business-form validation or unrelated reads.
- The expanded live check initially failed at "can you query the internet":
  the realtime-evidence gate treated a capability question as a fact query.
  Pure capability questions now use actual tool registration, including when
  public search was preselected, without gateway inference or irrelevant web
  reads. Specific weather/news/portal questions do
  not match this shortcut. Missing registration and temporary source failures
  remain distinct; OS-specific advice must state its applicable conditions.
- On the final tree, 111 general/stream/intent/scope/account-model tests and
  192 assistant/query/API/reliability/fast-path/boundary tests passed. Python
  compilation passed. The full preflight in the next section predates these
  changes and is not reported as a new full-tree run.
- The final nineteen-case configured-model check passed ordinary translation,
  writing, Python syntax, calculator callbacks, model identity, capability,
  retained chat code and latest-correction recall, real forecasts, public
  search and reading the actual official Python chapter. Warm replies were
  2.1-5.4 s; capability/identity returned without inference; forecasts were
  0.05-0.48 s, official search 17.97 s and chapter reading 5.84 s. Background
  preparation took 44.26 s. Earlier failed checks are not counted as passes.
- The default OpenClaw engine passed all eighteen synthetic-business scenarios
  with the real model: separate event/notice/repair counts, combined pending
  work, followups, water, cabinet totals, learning, daily tasks, drills,
  convergence, work-order reads and installed-skill/reference reading. Native
  statistics took 0.005-0.173 s; model-backed fixture queries took 2.4-7.3 s.
  These are isolated fixtures, not performance measurements of production DBs.
- All three owned gateway PIDs were checked absent. No production model/key,
  conversation or business record was modified. Frontend Key protections were
  not changed. Cold startup and broader real-data/integration accuracy remain
  open; these scenarios do not prove universal model factual correctness.

### Latest Windows Metadata And Final Preflight Evidence (2026-10-06)

- The Windows preload substitutes only the pinned SDK's exact readonly PID
  creation-time and listening-port probes. Creation times use the installed
  Win32 API through the current interpreter; ports use the SDK's existing
  netstat parser. Unowned/changed commands retain the original implementation.
  Failed or malformed results fall back within the original timeout budget.
  No live PID/port cache, skipped identity check or new dependency was added.
- The independent PowerShell reference agreed at millisecond precision. The
  Win32 query took 0.0461 s and the reference took 1.5108 s; the former excludes
  Python child startup and is not a gateway-startup measurement. Native tests
  cover both SDK identity readers, IPv4/IPv6 owners, unowned calls and failures.
  The runtime/startup/packaging group passed 91 tests; the page group passed 17.
- Two-account profiling passed before and after: initial/restart readiness
  was 52.971/48.527 s before and 49.678/51.361 s after. The first after run
  overlapped regression load. These observations do not prove an overall
  startup reduction; native imports remain the dominant cold-start limitation.
- The final twenty-account native gate passed concurrency with identical
  operation IDs, private models/keys/context/tools, automatic compaction,
  provider-overflow recovery, replay, lost terminal response, model switching,
  stop/auth-error isolation, prior-state upgrade and restart. Readiness was
  39.812 s, account preparation 85.26 s, concurrent processing 37.58 s and
  restart readiness 74.835 s. One loopback gateway remained shared. All six
  owned gateway PIDs from these runs were independently checked absent.
- The final production tree passed `package_portable.py --preflight-test`:
  118 Python files compiled, readiness/static checks, 53 notice identity,
  1763 learning/assistant (two optional network skips), 190 service/update,
  35 convergence, 123 notice/repair/lifetime, 33 guard and 18 transport tests.
  Native preload syntax passed separately. Git publication fixtures used only
  a private temporary file:// repository; no actual patch was published.
- VPN isolation was rechecked: opening/bootstrap/local rules never probes
  the intranet; remote failures preserve cache/credentials and show a local
  VPN message without blocking other requests or logging out of the portal.
  Real VPN access was not attempted. No production restart or business write
  occurred. Cold startup and universal model factual accuracy remain open.

### Latest Public Page Cache And General Advice Evidence (2026-10-06)

- Successful public pages can be reused across turns only when the server allows
  caching. Max-age and Age are honored, download/parse time is subtracted, and
  reuse is capped at sixty seconds and sixty-four pages. Private/no-store/
  no-cache responses, cookies, sensitive Vary values and query-bearing URLs are
  not cached. Question/URL checks run before lookup; copies retain the original
  queried_at. Explicit refresh bypasses caches without bypassing safety/budgets.
- On the final tree, the real MDN for-statement page took 2.5343 s to read and
  0.0003 s to reuse; both returned the same original reading time. Explicit
  refresh took 1.3323 s and performed a new read. Python documentation and the
  weather page did not satisfy cache eligibility in these runs; no speed gain
  is claimed for those sources. Failure results are not cached.
- The public/page/stream/query/intent/basic/scope/account-model group passed
  199 tests in 12.171 s with two optional network skips. This is the current
  scoped regression gate; the full preflight below predates these final changes.
  Production-dist Key protection passed again, including blocked copy/cut/
  context-menu/drag, permitted replacement paste and clearing after saving.
- Live acceptance now rejects unsourced numerical password advice. A stricter
  run failed at case five despite the original instruction; it is not counted
  as a pass. The final instruction explicitly distinguishes model memory from
  actual evidence and forbids adding a disclaimer after an unverified number.
  Default periodic rotation advice was also corrected against the NIST source:
  https://pages.nist.gov/800-63-4/sp800-63b/authenticators/ . This is guidance,
  not a change to portal password policy or a blanket override of employer rules.
- The final eleven-case configured-model run passed: gateway 37.752 s/background
  preparation 39.68 s, ordinary replies 2.09-4.54 s, forecasts 0.04-0.12 s,
  official documentation search 23.86 s and direct chapter reading 6.13 s. This
  verifies those scenarios, not universal factual correctness. Some general
  replies still confuse missing evidence with a missing capability and give
  version-specific operating-system advice without sufficient qualification.
- Two isolated profiling runs passed inference/callback, concurrent private
  models, switching, stopping and restart recovery. Initial/restart readiness
  was 52.635/52.866 s with CPU/spawn tracing and 42.695/52.477 s with filesystem
  attribution. SDK/dependency reads are mostly unique, while Windows process
  probes and native imports remain costly. API spans nest and must not be summed.
  No mutable-state cache or skipped security/migration check was introduced.
- Cold startup and initial public search latency remain open. No production
  restart, real business write, credential publication or real patch push was
  performed. All owned gateway PIDs from these probes were checked absent.

### Latest Public Read Recovery And Shared Gateway Evidence (2026-10-06)

- Public search hedges the existing precise Bing/Mwmbl sources; the backup starts
  after 250 ms. Broader retrieval remains a fallback and all results still pass
  the original topic, site and privacy gates. Returning or cancelling closes the
  other pending read. Official contents links remain navigation, not evidence of
  an unread chapter. Deterministic slow/unrelated/cancelled-source checks passed.
- Public page transport errors may retry once within the existing three-fetch,
  twenty-second turn budget. Successful reads and final failures are cached;
  unsafe URLs and permanent rejections are not retried. The index and chapter
  transient-failure reproductions failed before the change and passed afterward.
  The current stream/public/page/query/intent group passed 140 tests in 11.098 s
  with two optional network skips. TLS and URL authority checks remain intact.
- The final isolated configured-model run passed all eleven ordinary/public
  scenarios. Gateway readiness took 39.992 s and background preparation 42.32 s;
  ordinary replies took 2.10-5.23 s, forecasts 0.05-0.13 s, official search
  14.81 s and direct chapter reading 6.51 s. Actual Python chapter text was read.
  An earlier run failed at a transient page read; the new live diagnostics record
  safe public URLs and failure status without keys or business content. These
  observations do not guarantee public-source availability or universal model
  accuracy. In particular, the generic password response still gave unsourced
  numerical advice despite the current-source instruction; this is not certified
  security guidance. Cold gateway preparation remains an open speed limitation.
- The twenty-account native gate passed same-operation-ID concurrency, private
  context/model/tool authority, compaction/overflow recovery, replay without
  inference, deliberately lost completion-response recovery, model switching,
  stop/auth-error isolation, old-state upgrade and restart recovery. Initial
  readiness was 47.616 s, account preparation 96.92 s, concurrent processing
  34.24 s, completed replay 0.105 s and restart readiness 101.820 s. One loopback
  PID/port was used. All owned gateway PIDs were independently checked absent.
- Separate two-account profiling found only seven duplicate binary SDK reads
  among 2681 calls and negligible repeated UTF-8 reads; another loader cache was
  not added. Production-dist Key protections passed again. No production service
  restart, real business write, intranet authentication or publication occurred.
- On the final runtime/test tree, `package_portable.py --preflight-test` exited
  successfully: readiness/static checks, 53 notice identity tests, 1756 learning/
  assistant tests (two optional network skips), 190 resident service/update/proxy
  tests, 35 convergence tests including VPN isolation, 123 notice/repair/lifetime
  tests, 33 guard tests and 18 package transport tests. Publication fixtures used
  only a private temporary file:// Git repository. No real patch was published.

### Latest Default CA Loading Evidence (2026-10-06)

- Fresh-process profiling isolated slow default CA file loading: the standard
  factory with cafile took 22.740 s/21.688 s CPU, while the same factory with
  certifi's exact PEM contents took 0.043 s. The default helper now uses
  certifi.contents() and ssl.create_default_context(cadata=...), retaining the
  standard verification flags, TLS minimum, hostname checks and all 136 roots.
  A fresh production-helper run took 0.098 s and reused the context in 0.000 s.
  Custom CA files/directories keep their existing HTTPX policy. Invalid bundles
  fail closed and do not populate the cache; no certificate check was disabled.
- The 170-test public/page/URL/fast-path/runtime/host/pool group passed after
  replacing the outdated certificate mock with a deterministic off-loop gate.
  Two optional public-network unit tests were skipped; the explicit live probe
  separately passed all eleven ordinary/public scenarios. Its background gateway
  preparation took 54.9 s, ordinary answers 2.86-7.00 s, forecasts 0.05-0.12 s,
  official search 24.12 s and actual page reading 6.85 s. Cold gateway and search
  latency are still unresolved; CA loading improvement is not an overall SLA.
- The actual isolated CLI passed singleton ownership and exact-process cleanup
  (health 24.231 s under test load, shutdown 0.348 s). Production-dist model Key
  protections passed again; saved keys stay write-only and replacement inputs
  are cleared after saving. No real business writes or publishing were performed.
- A synthetic provider reproduced a separate account-isolation defect: a
  Set-Cookie response to account A reached account B through the shared model
  HTTP pool. The pool now rejects cookies with the stdlib CookieJar policy while
  retaining TCP/TLS reuse and per-request authentication. The deterministic
  cookie test failed before the fix; the final 67-test runtime/pool/TLS-policy/
  host group passed in 35.901 s. No new dependency or per-account port was added.
- The first package run loaded the previous CA helper while its mocks were
  being corrected; it stopped at four old-entrypoint assertions. The fresh
  `package_portable.py --preflight-test` retry completed successfully: 53 notice
  identity tests, 1748 learning/assistant tests (two optional network skips),
  resident service/update/proxy checks, 35 convergence tests, 123 notice/repair/
  lifecycle tests, 33 guard tests and 18 transport tests. The final cookie-policy
  change followed bulk-module collection and is additionally covered by the
  separate 67-test group and fresh source compilation above. No production
  service was restarted; publication fixtures used only private file:// Git.

### Earlier Transport And PC Evidence (2026-10-06)

- Default public and model HTTPS clients now share the verified CA context only
  when their CA policies are identical. Custom public CA files/directories remain
  separate from the model transport. On this host, the old two initializations
  measured 20.517 s and 23.039 s; a fresh changed-process run measured 15.632 s
  followed by 0.000 s, with the same verified context. These runs had different
  machine load; this proves removal of the second load, not a startup SLA.
- Public page reads honor the configured HTTP/HTTPS proxy while retaining the
  validated destination IP and original TLS hostname. Real memory-BIO TLS tests
  accept the trusted original hostname and reject a wrong hostname or an
  untrusted certificate. No CA/hostname verification was disabled.
- Unified worker startup now prepares the model HTTPS pool alongside the
  gateway, using the existing runtime client. Preparation performs no inference.
  Failure cancels its pending sibling task, and health remains available with a
  sanitized failure state. The isolated actual CLI passed singleton reuse and
  early shutdown after this change (health 25.944 s under concurrent test load,
  exit 4.016 s). This verifies lifecycle safety, not a cold-start improvement.
- The actual configured model passed eleven general/public scenarios: six
  ordinary questions (including tool calculation), Chinese/English Nantong
  forecasts, official documentation search and actual control-flow chapter
  reading. The earlier official-page transport failure did not recur in this
  run. Background preparation took 52.54 s; ordinary replies took 2.57-6.94 s,
  weather 0.05-0.13 s, official search 23.47 s and page reading 11.21 s. Search
  latency and cold gateway preparation still require attention.
- All eighteen configured-model/native-business scenarios passed against
  isolated readonly APIs. The first cold reply took 79.201 s; later provider-loop
  cases took 3.336-8.452 s and local deterministic cases 0.000-0.489 s.
- The production-dist Key browser gate passed again. Stored credentials never
  enter the browser form; replacement copy/cut/context-menu/drag events are
  blocked, paste remains available, and successful saving clears the input.
- The twenty-account pinned-runtime gate passed native context compression,
  overflow recovery, concurrent private model/tool calls, identical operation
  identifiers, model switching, stop/auth-error isolation, old-state upgrade,
  restart recovery and deliberately lost completion-response recovery without
  inference/tool replay. This loaded-machine run measured gateway readiness
  59.813 s, account preparation 129.83 s, concurrent two-round processing 52.85 s,
  completed replay 0.162 s and restart readiness 125.801 s. It overlapped unit
  suites and is not a comparable speed benchmark. All four real-model/shared
  gateway PIDs from this iteration were checked absent after cleanup.
- Notice detail shells now retain their PC layout while their bodies refresh.
  Equivalent return-navigation defaults reuse the existing list; source-version
  changes still refresh it. Transient connection failures preserve the page,
  same-instance recovery removes the warning, and a changed backend instance
  still requires refresh. Slow SQLite stream reads run off the HTTP event loop.
  Six notice types, slow/late reads, navigation and shared connection probing
  passed isolated production-dist browser checks; 200 related backend tests
  passed in the notice iteration. Event notice submission logic was not changed.
- Broad assistant regression exposed an intermittent water-photo retry defect:
  a generated file UUID containing phone-like digits was scrubbed when saved
  into the review form. A deterministic two-ID reproduction failed before the
  fix. Validated file selections now retain their original opaque IDs; free-form
  text continues through the privacy filter. The 118-test water/file/review/
  cabinet-proof/drill-upload group passed afterward, including expired staging,
  retry and one final native write. The unified host/client/proxy/startup group
  also passed 118 tests after parallel model-transport preparation.
- The corrected full assistant module run passed 1617 tests in 516.324 s after
  the deterministic file-ID fix (1615 passed, two optional public-network tests
  skipped). The package preflight passed its 1746-test assistant/learning group
  and 190-test worker/update/proxy group, but stopped later at the independent
  convergence browser-login cleanup test. That test also failed when isolated;
  the complete package preflight must not yet be claimed successful.
- Convergence authentication diagnostics showed the captured token entering
  verification within three seconds, but the one-shot HTTP client inherited the
  machine proxy and rebuilt CA contexts. Validation for this fixed intranet API
  now bypasses environment proxies, with normal TLS verification unchanged.
  Its loopback fixture also now consumes and checks the JSON request body before
  responding, avoiding an unread-body connection reset race. Actual intranet
  authentication has not been contacted or certified by these local checks.
- After the authentication and fixture corrections, the exact remaining package
  gates passed together: 206 convergence, submission, event atomicity, notice
  upload/undo, repair identity/cache, process lifetime, guard and transport tests
  in 358.462 s. The earlier package command itself exited before these repairs;
  it has not yet been rerun end to end. Transport publication used only a private
  local file:// Git fixture, not Gitee or any production artifact.
- Run all assistant unit modules by package name (`bin.test_lighthouse_*`), for
  example by enumerating their filenames into `python -m unittest` arguments.
  `bin` is a namespace package; plain `discover -s bin` drops its package context
  and falsely fails seven relative-import modules. No application code should be
  changed to accommodate that incorrect test invocation.
- Plan convergence stays offline on initialization: no connectivity probe,
  remote list refresh or browser login runs until explicitly requested. The
  page explains the host VPN requirement, and unreachable Zhihang requests
  return a feature-local 502 without invalidating portal login, credentials or
  cached records. An actual ASGI concurrent request fixture checks that a held
  Zhihang timeout leaves other routes and cached reads responsive. Local rules
  remain usable. The 35 convergence/adapter tests and 38 assistant plan/query
  tests passed; typechecking, production build and PC browser checks passed.
  No real VPN/intranet connection was attempted.
- The separate two-account startup diagnostic passed retained history, private
  model switching, concurrent tools, stop isolation and gateway restart. It
  measured 92.543 s initial gateway preparation and 59.185 s after restart;
  native SDK configuration/environment imports and Windows/file I/O dominate
  the observed profile. Diagnostic spans overlap and must not be summed. No
  additional SDK shortcuts were applied; cold latency is still unresolved.
- No real Feishu business test data was written, production service restarted,
  or patch/runtime published by these checks. Arbitrary model answers and all
  live business integrations are not certified by synthetic fixture results.

### Previous Shared-Gateway Acceptance (2026-10-06)

- The runtime explicitly loads the same two allowed plugins, `openai` and
  `lighthouse-tools`, instead of scanning the unused bundled inventory.
  The pinned provider plugin is already included by the runtime packager.
  Authentication, state migration, foreign-database checks and tool restrictions
  remain unchanged; no SDK source was patched and no dependency was added.
- Two-account SDK diagnostics measured plugin metadata scans around 0.1-0.24 s
  after this change, versus around 0.75-1.67 s before it. Spans overlap and must
  not be summed. Overall cold startup remains variable and slow.
- Periodic synchronous compile-cache flushing was tested and rejected: gateway
  startup rose to 158.615 s and individual flushes stalled Node for 20-37 s.
  That experiment exists only in the opt-in diagnostic tool, not the runtime.
- General coding questions containing identifiers such as `A_B` no longer fail
  because those identifiers resemble building codes. The existing general-topic
  classifier is reused; actual business queries still enforce building authority.
- Full assistant regression: 1612 tests in 613.931 s, 1610 passed and two opt-in
  public-network checks skipped. The unified startup, storage, proxy, update and
  lifetime group separately passed 188 tests in 280.977 s.
- The actual pinned-runtime twenty-account gate passed native compaction,
  provider-overflow recovery, simultaneous private model/tool calls, completed
  replay, model switching, account-stop/auth-failure isolation, preceding-state
  upgrade and restart with retained private history. One completed observer
  response was deliberately discarded; its original result was recovered with
  exactly forty provider requests and twenty callbacks in the concurrent phase.
  No inference or business tool was replayed.
- That run measured initial gateway readiness 57.747 s, twenty-account
  preparation 107.17 s, concurrent two-round calls 35.18 s, completed replay
  0.096 s and restarted gateway readiness 116.813 s. These are synthetic-provider
  measurements, not cloud-response SLAs or proof that the earlier intermittent
  concurrent observer timeout has been eliminated.
- The configured real model passed all eighteen native question scenarios,
  covering general conversation, authorized knowledge, separated business
  counts, unfinished work, follow-ups, water, cabinets, learning, daily tasks,
  drills, convergence, readonly work orders and installed skill/reference reads.
  All business endpoints were isolated readonly fixtures. The first reply took
  69.315 s including cold startup; subsequent provider-loop scenarios took
  3.493-9.807 s. Local deterministic scenarios took 0-1.006 s.
- Release readiness, affected Python/Node syntax and whitespace checks passed.
  The production-dist Key browser gate passed: stored credentials were absent
  from the input, replacement copying/cutting/dragging was blocked, save was
  locked while pending and the replacement was cleared afterward. All three
  owned native gateway PIDs had exited after their fixtures completed.
  These checks do not certify all live Feishu
  integrations or arbitrary model answers. No production service was restarted,
  real business test record written, runtime mirror published or user patch
  packaged/published by this iteration.

### General Assistance And Authorized Knowledge (2026-10-06)

- Official-source fallback recovers the publisher's subject from the original
  user question when a model omits it from the query. Publisher filtering,
  privacy checks and the existing overall search deadline remain in force.
- Conceptual UPS/HVDC/CRAC/CRAH and related electrical questions may read only
  the existing authorized question-bank/material APIs. Native visibility and
  opened-answer rules still apply; unrelated business reads and study writes
  remain blocked. Accounts without study access use general knowledge instead.
- Exact greetings do not authorize an unrelated business read. The wider
  non-business guard was rejected after it broke four continuation/navigation
  cases; original business continuation behavior has been restored.
- Selected weather queries recognize English relative dates and reject mixed
  dates or contradictory explicit/relative dates instead of returning today.
- Full assistant regression: 1572 tests, 1570 passed and two opt-in network
  tests skipped. Production-dist key browser check and release readiness passed.
  The browser receives no saved key, blocks copying/cutting/dragging a replacement
  key, permits pasting and clears the replacement after a successful save.
- The real configured-model probe passed nine general/public scenarios. A
  separate real model/native-tool fixture passed a natural UPS question using
  authorized synthetic knowledge, with no unrelated business reads or writes.
- Instrumented live run: cold preparation 63.93 s; first question 16.71 s,
  reaching the broker after 3.74 s and waiting 12.51 s for upstream headers.
  The latter includes initial HTTP-client construction, connection and provider
  response, not measured inference alone. Later ordinary replies took 2.19-3.24 s.
  First weather took 6.89 s, cached weather 0.04 s, official search 6.32 s.
- No production settings/conversations were modified, no Feishu business test
  records were written, and no patch was packaged or published.

The dedicated real-runtime compaction gate below has now passed. The earlier
intermittent twenty-account connection reset remains unexplained and was not
reproduced in this final run. These checks are progress toward the larger
assistant objective, not proof of every live business integration or answer.

### Native Context Compaction And Recovery (2026-10-06)

- The broker retains fixed context-overflow/error codes instead of stripping
  recovery semantics. Error-body reads are bounded to 16 KiB and two seconds;
  provider details, credentials and record identifiers never pass through.
- Recovery compaction in the pinned SDK does not always emit WS progress.
  The broker marks an actual native summary request for its owning run only;
  the stream reports that status once without relaxing run-event filtering.
- `check_lighthouse_context.py --accounts 20` passed with the pinned real Node
  runtime and synthetic loopback providers. It checks automatic compaction,
  private memory recall, progress isolation, a forced provider context rejection,
  twenty simultaneous model requests/callbacks, private model/key switching,
  stopping one account, and memory recall after a real gateway restart.
- Synthetic replies respect the configured output budget. Summary fixtures
  follow the SDK's required section format; no quality guard was disabled.
  The fixture uses a fresh operation for each phase, while retaining identical
  operation IDs across different accounts to verify account-bound idempotency.
- This run measured 123.11 s for twenty-account cold preparation, 35.49 s for
  the concurrent two-round model/tool fixture, and 96.62 s for restart readiness.
  These measurements are not real-provider latency guarantees. Preparation
  continues in the background, outside Qt and portal readiness.
- Full assistant regression: 1572 tests, 1570 passed and two opt-in network
  skips. Production-dist credential browser checks, release readiness, syntax
  and whitespace checks passed. No production service, business record or
  published patch was changed by these tests.

### Completed Results, Model Errors And State Upgrade (2026-10-06)

- A real completed-run replay reproduced the old ten-second fixture timeout:
  the native RPC returned its final outcome without new streaming events.
  Completed outcomes now read the exact original run's terminal reply. Missing,
  suppressed, wrong-run and failed outcomes stop without new inference or tools.
  Fresh accepted/in-flight runs keep the existing streaming path and avoid the
  extra observer RPC.
- Native model authentication/rate-limit/request/timeout/connection/context
  failures use fixed actionable messages and persisted error categories. Raw
  provider bodies, keys and identifiers are never shown. Unknown errors remain
  a sanitized model-run failure rather than claiming a business result.
- Injected authentication failure exposed a restart defect: the private Agent
  SQLite lived outside the previous shared-gateway state boundary. The native
  migration refused readiness. State ownership now covers the existing accounts
  folder; `session.store` explicitly freezes the preceding per-Agent paths under
  shared-gateway/agents/. No database is deleted, no signature/key/business data
  is moved, and the native foreign-database guard remains enabled.
- The real two-account `--replay --faults --legacy-state` fixture passed the
  previous-layout first launch, completed result recovery, private callbacks,
  model/key switch, single-account stop, actionable authentication failure with
  another account still usable, and restart with the original context retained.
  Completed replay took 0.335 s in this final fixture (0.135 s in the earlier
  failure-isolation fixture); neither called the provider or tool again.
- Latest focused runtime/native/service/pool regression: 89 tests passed.
  Stream/basics/model-account/startup/service regression: 108 passed. The focused
  native/stream/transport suite passed 90 tests. These overlap and must not be
  summed or presented as a fresh full-suite run. Release readiness passed.
- First model-client construction was an observed latency gap. A no-network timing
  probe measured 12.088 s overall and one TLS-context build taking 11.048 s.
  Proactive verified transport preparation was subsequently implemented and
  verified below. TLS verification was not disabled.
- No production service was restarted, no real business table was written,
  and no user patch was packaged or published during these probes.

### Verified Transport Preparation (2026-10-06)

- Startup preparation overlaps runtime checking, account gateway warmup and
  verified model-HTTP preparation. It does not infer or request business data.
  The first provider call no longer constructs a client or loads CA certificates.
- CA contexts are cached separately for public-query environment policy and
  strict model-provider policy. Both retain certificate and hostname verification.
  The existing forty-connection/twenty-keepalive model pool, no redirects and
  per-request authorization/empty Cookie header remain unchanged.
- Cancellation closes any unpublished client; closing during certificate work
  cannot create a late client. Model requests recheck the owning account after
  preparation, so stopped, replaced or closing requests do not reach a provider.
- Focused runtime/public/native/startup/pool tests: 140 run, 138 passed and two
  opt-in network skips. Production-dist Key browser checks passed. The route
  fixture implements preparation without starting Node or making HTTP requests;
  its interface and the old private TLS-cache test were updated, not bypassed.
- The real twenty-account combined fixture passed compaction, provider-overflow
  recovery, completed replay, concurrent private calls, model/key switch,
  single-account stop, authentication-failure isolation and previous-state upgrade
  with retained history. Cold account preparation measured 116.03 s, concurrent
  two-round fixture 35.87 s, completed replay 0.117 s and restart 108.86 s.
- A real configured-model run passed nine general/public scenarios. Final run:
  background preparation 71.82 s; first ordinary reply 5.27 s, arriving at the
  broker after 3.91 s with 0.78 s to upstream headers. Subsequent ordinary replies
  took 2.17-3.06 s. First weather was 1.11 s, cached weather 0.04 s.
- These are local observations, not SLAs. The pinned gateway cold start remains
  slow but off the portal/Qt path. Official documentation search is still weak:
  one run repeated four searches and took 17.12 s, returning generic/older official
  documentation pages rather than the requested chapter. Better source retrieval
  and preventing unproductive search loops remain full-objective work.
- No production business records, model settings, conversations or published
  patches were modified by these probes. Final full assistant regression ran
  1582 tests: 1580 passed, two opt-in public-network tests skipped. The corrected
  TLS-policy/cache tests were rerun rather than adding a runtime compatibility
  path. Production-dist Key checks and release readiness passed.

### Search Topic Retention And Turn Budget (2026-10-06)

- Reproduction tests exposed three search defects: publisher/domain words alone
  qualified generic pages, RSS results were truncated before relevance ranking,
  and a shortened fallback discarded the original requested topic. Ranking now
  excludes site words as topic evidence, filters before the final five-result
  bound, and verifies every fallback against the original query and publisher.
- Each turn owns its search-result cache and budget. Identical case/whitespace
  variants reuse the result; repeated evidence tells the model not to search
  again. At most three distinct attempts share a twenty-second search window.
  Timeout cancels the request and prevents another fetch. Source authority,
  public/private input gates, response byte limits and redirect policy are intact.
- The final focused public/stream suite ran 82 tests: 80 passed, two opt-in
  network tests skipped. Two separate turns independently received their budgets;
  a timed-out fetch was cancelled once without a second network request.
- Live generic control-flow search returned relevant topical pages in 2.53 s.
  The specified official publisher did not return the requested chapter in this
  run; after 1.06 s it correctly reported unavailable rather than citing a generic
  index as chapter evidence. Better primary-source retrieval remains open. No
  new search dependency was installed and no real business data was submitted.
- Unified entry/launcher/client/update/portal/package/pool/log regression ran
  80 tests, all passed. Release readiness passed. Full assistant regression ran
  1587 tests: 1585 passed, two opt-in public-network tests skipped. The final
  public/stream subset was also rerun after diagnostic deduplication; these
  overlapping subsets are not summed.

### Public Page Reading And Long Text Responsiveness (2026-10-06)

- Added the typed public_page tool and slash-menu entry. Only this turn's user
  URLs, actual search/navigation candidates and returned same-site links authorize
  a fetch; navigation candidates are not evidence until their content is read.
  Reads are per-turn cached, with three distinct pages and a twenty-second window.
- Dynamic HTTPS targets require verified public DNS addresses. The socket uses
  the checked literal IP while Host/SNI retain the original hostname; certificate
  verification is unchanged. Proxy benchmark DNS uses fixed Ali/Cloudflare/Google
  DoH services with strict question, CNAME and public-address checks, never the
  page path or business context. Ordinary private/mixed/mapped/tunnel DNS fails
  closed. No machine DNS/VPN/proxy settings were changed.
- No cookies, credentials, redirects, page scripts or binary downloads are used.
  Page input is limited to 512 KiB, returned text to 6000 characters and same-site
  links to forty. Larger pages are explicitly partial. Parsing runs off the event
  loop; the final renderer corrects an unsupported claim of reading the full text.
- A large-page test reproduced quadratic email matching in the shared privacy
  filter. An email-start boundary makes the scan linear without removing contact
  protection; 512 KiB of unbroken text took 0.186 s locally. The two owned regression
  processes using the old regex were stopped after CPU and scaling measurements,
  not because an observation timed out. Their interrupted runs are not passes.
- Real primary-source reads succeeded: the control-flow chapter in 1.44 s, and
  a large official contents page in 2.68 s with its actual chapter link. DNS and
  HTTP substitutes cover rebinding, private/mixed addresses, malformed DoH,
  redirects, compression, limits, cancellation and TLS/hostname retention.
- The strengthened real-model gate passed ten scenarios, requiring the actual
  official chapter text, not merely an official hostname. Background preparation
  was 65.72 s; ordinary hot replies 2.2-2.7 s, search/navigation 14.24 s and direct
  URL reading 7.50 s. Public-query validation now names the required public tool
  instead of asking the model for business counts. These are observations, not SLAs.
- Weather in that run used wttr's nearby Dexing station, explicitly not an exact
  Nantong observation. No false historical/current observation was asserted.
- Full assistant regression after the transport/privacy fix: 1599 run, 1597 passed,
  two opt-in network skips. Final excerpt rendering and DoH HTTP-fallback changes
  received focused tests. Fresh final full gate: 1600 run, 1598 passed, two opt-in
  network skips. The excerpt correction was moved to the common final renderer
  after streaming-fixture evidence; both native and Pydantic responses now use it.
  Production-dist Key browser protection and release readiness passed. No real
  business data, settings, conversations or published patch were changed.

This closes the demonstrated chapter-retrieval gap, not the whole assistant goal.
Cold startup and wider real business/answer quality acceptance remain open.
Direct inspection also reproduced two weather-routing gaps: the Chinese request
for next week wrongly selected today's fast path, while ordinary English weather
did not select the fast path. These require the next intent/date iteration, not
more source retries or fabricated forecasts.

### Weather Dates, Cited Knowledge And Topic Navigation (2026-10-06)

- Date-routing tests reproduced next-week queries incorrectly using today and
  ordinary English weather bypassing the native fast path. Chinese/English
  dates now reject invalid, conflicting or unsupported periods instead of
  silently selecting today. Date-only follow-ups inherit weather context;
  translations and unrelated next-day business questions do not.
- Explicit citation requests require a real authorized knowledge or public
  source. Model memory and task hints cannot substitute for a source. Normal
  source-code explanations do not trigger unnecessary public searches.
- A live failure showed the model repeatedly searching after receiving an
  official contents link. Search now reuses the existing verified page reader
  to read an actual returned index and its relevant returned chapter link.
  Navigation alone remains unavailable, not subject evidence. Resolved results
  are cached within the original per-turn page/search budgets.
- The configured-model native fixture passed fourteen read-only scenarios:
  general dialogue, model identity, cited equipment knowledge, events, repairs,
  follow-ups/progress, water, cabinets, learning, daily tasks, drills,
  convergence, work-order queries and an actually read registered skill.
  These business APIs contain synthetic data, not live business acceptance.
  Hot local count paths took 0.004-0.282 s; other fixture queries took 2.425-8.102 s.
- The configured-model public fixture passed eleven scenarios, including actual
  official chapter text and English next-day weather. Final observed search
  reply took 9.58 s after early navigation, versus 18.08 s in an earlier run.
  Ordinary hot replies took 2.69-3.40 s; weather took 0.70-1.72 s. Cold preparation
  was 56.78 s. These observations are not latency or answer-quality guarantees.
- Weather still explicitly identified the nearby Dexing station rather than
  claiming an exact Nantong observation. No production conversations, model
  settings, Feishu records or published patches were changed by these probes.
- Focused final public/stream/date/basics regression passed 110 tests with two
  opt-in network skips. Production-dist Key protection, syntax checks and
  release readiness passed. The full final gate is recorded separately below.

### Current PC And Cross-Module Query Iteration (2026-10-06)

- The actual resident Python/Node proxy and production-dist PC fixture passed
  input clearing, private upload/download, retained-context model switching,
  portal restart without replay, stop/continue and in-flight supplementation.
  A human-confirmed synthetic rule write executed once; repeated confirmation
  did not repeat it. No real Feishu business record was written.
- Native fixture timings: cold reply 51.760 s, hot reply 1.980 s, shutdown
  2.373 s. Concurrent portal request P95/max were 3.6/6.4 ms during preparation
  and 3.5/3.7 ms when hot. The desktop screenshot was inspected; no page error
  or horizontal panel/page overflow occurred. This fixture proves the chat
  surface, not every live business page's load time or the whole avatar layout.
- Extended native acceptance verifies seven current notices, 250 pending plans
  across two pages, two pending change plans, one pending repair plan, five
  ongoing repair notices and three unfinished repair projects. Combined pending
  work also includes events, cabinet batches, SOP queries, MOP files, drills and
  learning, with complete source counts and zero groups omitted from the reply.
  These native local reads took 0.005-0.041 s in the final fixture, not production
  timing guarantees. Every fixture still denies non-E scope queries.
- The first eighteen-case live run passed seventeen cases but a sourced UPS
  response only listed its reference. A reproduction test now verifies one
  bounded correction without repeating the source read; an explanation followed
  by its citation is accepted normally. Authorized-source requirements remain
  unchanged. The targeted real-model rerun and final full eighteen-case live
  rerun both passed, including the actual registered skill/resource read.
- Current counts/paging/scope/reliability regression: 88 passed. Knowledge,
  streaming and boundary regression: 61 passed. These overlap and are not a
  full-suite result. Final real-model hot queries took 2.468-9.881 s; ordinary
  cold reply was 59.925 s, so preparation remains a known speed limitation.
- Key-browser protection, release readiness and affected-source syntax checks
  passed. No production settings, conversations, cloud writes or user patch
  publication were performed. The full final regression is recorded separately.

### Previous Citation Regression Gate (2026-10-06)

- With the citation-only explanation fix in place, the complete assistant suite
  ran 1607 tests in 403.070 s: 1605 passed and two opt-in network checks skipped.
  No runtime changes followed this gate within that iteration. The eighteen-case live native probe,
  targeted knowledge rerun, production-dist Key checks, syntax and release
  readiness checks also passed with that iteration's source.
- The broader objective remains active. Cross-module fixtures prove their
  contracts, permissions, paging and counts; they do not certify every live
  Feishu integration or arbitrary model answer. Cold gateway preparation remains
  slow but outside Qt/portal readiness. No real business test write or patch
  publication was performed.

### Shared Gateway Process And Completion Recovery (2026-10-06)

- The pinned launcher's packaged-cache relay is suppressed using its existing
  environment flag. Its versioned compile cache remains enabled. The process
  tracked by the host now directly owns the gateway listener; configuration,
  migration, authentication and foreign-database checks remain unchanged.
- Native profiling attributed substantial cold-start time to SDK code reads,
  package metadata and synchronous subprocess work. Instrumentation observed
  2726 main-thread code reads, about 34.5 MiB and 6.4 seconds; CPU samples also
  included spawnSync, package metadata and file open/read/close. This is not a
  complete profile of every short-lived subprocess or proof of faster startup.
- The two-account direct-PID fixture passed listener ownership, concurrent
  callbacks, completed replay, model/key switching, stop isolation, model
  authentication failure, previous-state migration and retained-history restart.
- One twenty-account combined run timed out after ninety seconds in the
  completion-observer stage while another native fixture was running. The cause
  is unproven. An isolated diagnostic rerun passed all phases, including native
  compaction and provider-overflow recovery: cold preparation 134.92 s,
  concurrent two-round calls 41.91 s, completed replay 0.095 s and restart
  readiness 129.09 s. This does not certify elimination of intermittent timeouts.
- Completion-observer connection errors may recover the original accepted run
  once through the existing readonly observer. Both recovery and normal final
  verification reject a different run ID. They never submit another agent run,
  repeat inference or replay a business tool. Focused regression: 125 passed.
- The production-dist PC fixture passed retained context/model switching,
  private attachments, input clearing, restart without replay and stop/continue.
  Cold/hot reply took 100.542/2.622 s; portal P95 stayed 4.3/3.9 ms. Actual direct
  gateway idle private memory measured 465.5 MiB, not the small cache-relay PID's
  memory; earlier relay-only samples must not be treated as gateway resource use.
- Native Windows lifecycle passed singleton startup, hidden priority, migration,
  update hold and parent-exit cleanup. Release readiness and saved-Key browser
  protection passed. These tests use owned temporary state and synthetic
  providers; no production restart, Feishu business write or patch publish ran.

### Previous Shared-Gateway Regression Gate (2026-10-06)

- With the direct-PID and exact-run observer recovery changes in place, the full
  assistant suite ran 1609 tests in 510.600 s: 1607 passed and two opt-in network
  checks skipped. No production-source change followed this gate within that iteration.
- The isolated real pinned-runtime twenty-account fixture passed listener
  ownership, identical operation IDs with private callbacks, completed replay,
  private model/key switching, single-account stop, authentication-failure
  isolation, preceding-state upgrade and retained-context restart.
- This fixture discarded one owned completion-observer response after the
  gateway had completed it. Recovery returned that same result with exactly
  forty provider requests and twenty callbacks in the two-round concurrent
  phase: no extra inference or business execution. Concurrent phase 52.62 s,
  cold account preparation 127.85 s, completed replay 0.095 s; restarted gateway
  readiness 124.48 s. These synthetic-provider timings are not production SLAs.
- Production-dist Key browser checks, Python/Node syntax, changed-source
  whitespace and release readiness passed. Test processes cleaned up; no real
  business write, production-service restart or user-patch publication occurred.
- The architecture requirement is implemented. The broader assistant objective
  remains active: cold preparation is still slow, the prior concurrent timeout's
  cause is not established, and these fixtures do not certify all live Feishu
  integrations or arbitrary model answers.

### Previous Business Response And PC Gate (2026-10-06)

- A 5,296,925-byte synthetic business response reproduced event-loop starvation.
  Twenty concurrent inline conversions stalled a ten-millisecond heartbeat for
  4514.5 ms. Moving all conversions into unconstrained worker threads alone still
  stalled it for 3990.4 ms and increased total processing time; that was not
  accepted as the fix.
- Response parsing/redaction now runs in the existing worker pool with a
  catalogue-local conversion lock. Only CPU-heavy conversion is bounded;
  network calls, business handlers and model inference are not put behind it.
  A barrier test proves twenty requests enter their native handlers together
  while conversion concurrency remains one. JSON failure envelopes, original
  rows/pagination and binary limits retain their original semantics.
- Final twenty-response conversion probe: all 500 original rows survived each
  response, elapsed 4297.2 ms and maximum heartbeat gap 47.7 ms. Including the
  real JSONResponse bridge encoding gave 4294.8 ms and 62.3 ms. These are local
  synthetic measurements, not live-cloud latency or an all-page performance SLA.
- The front-end API audit found 148 covered call shapes, no definite missing
  shape, sixty intentional exclusions and twenty-seven unresolved static calls.
  The separate twenty-test dynamic-caller review passed. This proves catalogue
  coverage, not every real integration. Original security, learning-query-only,
  Qt-event-write and work-order-execution exclusions remain in effect.
- The complete assistant suite passed after the response change: 1611 tests
  in 325.065 s, 1609 passed and two opt-in network checks skipped. Focused
  API/business/scope/paging/reliability regression passed 104 tests; seven
  deterministic native-query scenarios passed, including separated notice,
  repair and pending-plan counts and the cross-module unfinished-work overview.
- The production-dist PC probe with actual Python service and pinned Node
  passed input clearing, private attachments, model switching/context, portal
  restart without replay, stopping/continuing and in-flight supplementation.
  Cold/hot reply measured 81.466/4.360 s, shutdown 2.731 s; portal P95 during
  cold/hot phases 3.8/3.7 ms. No browser page error occurred; the final chat
  screenshot was inspected. This fixture does not establish whole-avatar layout
  or every live business page's rendering behavior.
- Saved-Key browser protection, release readiness, Python syntax and whitespace
  checks passed. No production restart, cloud business test write or patch
  publication ran. The broader objective remains active: cold gateway startup
  and the earlier intermittent concurrent observer timeout are not fully solved.

### Previous Final Regression Gate (2026-10-06)

- After the final navigation-result schema fix, the complete assistant suite
  ran 1606 tests in 331.584 s: 1604 passed and two opt-in public-network checks
  skipped. No runtime changes followed that gate within its iteration.
- The unified-entry/service/client/store/update/proxy/transport/package group
  ran 146 tests in 140.803 s, all passed. Production-dist Key protection,
  release readiness and affected-source syntax checks also passed.
- These gates validate the shared-gateway implementation and demonstrated
  regressions, not all real-business integrations. The larger assistant goal
  remains active; cold preparation and wider production answer-quality evidence
  remain limitations. No production service restart, cloud business test write
  or user patch publication was performed.

### Shared Gateway And Private Agents (2026-10-06)

- One loopback Node gateway process and port serve all accounts. Each account
  retains a distinct agent ID, workspace, session history, model profile and
  server-authorized tool callback. Model credentials stay in the Python broker,
  not in Node configuration, environment or owned diagnostic files.
- Twenty active accounts may infer concurrently. Cold registration is shared;
  stopping a single account never terminates the gateway or another account.
  Model/key changes retain the account's history without restarting the gateway.
- The real pinned Node fixture passed twenty simultaneous provider requests and
  private callbacks, identical operation IDs across accounts, private model/key
  switching, stop isolation and gateway restart with retained history. It uses
  synthetic loopback providers, not production credentials or Feishu writes.
- This run measured 107.83 s for twenty-account cold preparation and 36.33 s
  for the simultaneous two-round model/tool fixture. Restart readiness took
  93.13 s. These are local measurements, not a response-time guarantee.
- An earlier run had transient connection resets in the concurrent phase;
  the full rerun passed. The cause is not established. Separate idle-connection
  probes did not reproduce the reset, so production timeouts were not changed.
  This result does not certify elimination of intermittent transport failures.
- Runtime/model/isolation checks: 64 tests passed. Startup/logging/store/launcher/
  packaging checks: 89 tests passed. Release readiness passed. The gateway
  fixture now starts and closes its HTTP server on the same event loop.
- No production service was restarted and no patch was packaged or published.
  These results certify this gateway change, not every live business integration
  or every public-query answer in the larger assistant acceptance objective.

### Query And Notice Latency Fix (2026-10-05)

- Simple weather queries and complete non-event start notices use native paths;
  they do not wait for a model/gateway round. Builtin guide selection no longer
  disables existing deterministic business counts. Business writes still need
  the original user confirmation and permissions.
- Private loopback HTTP and in-process ASGI clients no longer build unused TLS
  contexts. Public HTTPS reuses one verified CA context and a connection pool;
  certificate verification and no-redirect/input gates remain enabled.
- Ongoing notice detail reads its latest authorized local projection without
  querying the full plan list/statistics again. Plan binding candidates retain
  their existing on-demand picker.
- Native command expansion preserves manual_id for independent starts. Tests
  exercise the real command expansion and action-job creation/deduplication.
  The assistant review renders business labels without internal polling fields.
- Final combined regression: 418 tests passed (two opt-in network skips).
  Live read-only Nantong weather returned from China Weather in 0.798 s, then
  0.064 s with the warm pool. These timings exclude one-time CA preparation.
- Real isolated Node/proxy probe: cold 61.989 s, hot 1.076 s, stop 0.427 s;
  portal P95 during cold preparation 3.0 ms. Full gateway cold start remains
  slow and is not claimed fixed by native fast paths. No real model key or
  Feishu business write was used in the gateway/notice probes.
- Startup trace attributed about 20.1 s to post-bind plugin loading and 8.6 s
  to reply-runtime loading in an isolated run. Removing the OpenAI plugin in a
  temporary probe did not improve readiness and was not adopted. Default
  logging is unchanged; stage tracing is opt-in for local diagnosis only.
- All six notice kinds use original form metadata for chat and review labels.
  Adjustment progress is retained in the native payload but hidden in chat;
  event upload and the original work-order execution protocols are unchanged.

- The composer has a searchable slash picker for guides and registered native
  tools, keyboard selection, paging and per-message frozen selections. Picking
  a tool never executes it; original business permissions and confirmation apply.
- Eighteen packaged guides include fifteen module guides; reviewed WorkBuddy
  guides remain available. Discovery links real schemas to relevant guides.
- External SKILL.md / single-skill ZIP uploads form a shared installed library,
  readable by all authenticated accounts. Only its creator or an administrator
  may remove a guide. Models, conversations and business scopes remain personal.
- Archive files are bounded, validated and stored as text-only guidance plus
  Markdown references. No scripts execute or authority is granted. The metadata
  index and per-guide content use atomic document batches in the assistant store.
- Desktop chat opens wholly to the left or right of the bot. Opening/closing
  does not move or resize the bot, and the composer has no icon clearance logic.
- Final combined regression: 305 tests passed. A source-lock timing assertion
  exceeded its wall-clock bound during concurrent builds; standalone and final
  sequential runs passed without changing the production timeout or assertion.
- Vue typecheck, production build, release readiness and notice static smoke
  passed. Production-asset browser fixtures cover keyboard/search, retained
  retry selections, shared install/remove, two desktop sizes, 200px bot, left and
  right opening with stable bot coordinates, and native-modal interaction.
- Real Windows lifecycle probe passed singleton ownership, hidden below-normal
  priority, one-generation migration, update hold, clean stop and parent exit.
  A real Node gateway/proxy probe passed SSE, typed native querying, gateway
  reuse and one human-confirmed synthetic write. Cold gateway preparation was
  still slow (72.9 s in that run); portal-query P95 was 3.4 ms during preparation.

All cloud/model/business fixtures were isolated. No production Feishu records
were written and no update patch was published. External executable skills and
live third-party model/provider reliability are not certified by these checks.

## Archived Independent-Service Results

The following results concern the previous design, not the current startup
contract. In particular, surviving main-process exit is no longer desired.

Date: 2026-10-04. This was the resident-backend objective. Earlier checks
in lighthouse_upgrade_acceptance.md concern the previous in-process migration;
they are not proof that this larger objective is complete.

## Earlier Verified Baseline

- Assistant implementation and assets live under bin/openclaw_service/assistant.
  Old module paths are one-generation import aliases, not duplicate instances.
- Main assistant routes are authentication/proxy adapters; business authority
  remains in the main portal. The independent service owns assistant documents.
- 113 isolated service, store, launcher, proxy, update, import and startup tests
  passed. Store fixtures cover assistant-only backup/migration, source changes,
  interrupted migration, attachments, path escapes and corrupted documents.
- A real Python resident process and pinned Node/OpenClaw gateway used synthetic
  loopback model and business APIs through the actual main portal proxy.
  SSE, typed native querying, human-reviewed proposal, one write, repeated
  confirmation, assistant-only storage and credential-free owned files passed.
- That native probe shut down and restarted the portal listener. The resident
  host and gateway PIDs remained unchanged and the conversation was retained.
- Native probe timings: cold question 79.78 s, reused-gateway question 10.74 s,
  authenticated manual shutdown 0.372 s. These are one local measurement, not
  an SLA or evidence for closing a Windows console window.
- The main portal stayed healthy after the assistant stopped; its assistant
  endpoint returned the manual-stop error without launching another process.
- Legacy hashed gateway directories move into accounts/ without changing their
  history bytes. Conflicts are checked before any move and are never overwritten.
- Control reconnection is bounded and respects manual stop/update holds. It
  reconnects authorization only and does not resubmit chat or business messages.
- Qt patch completion is emitted only after the assistant update guard finishes,
  including rollback/failure paths; shared-dependency updates are covered.
- Duplicate manual BAT calls can observe the existing instance's logs and issue
  a stop bound to that instance. Replacement-instance rejection is unit-tested.
- DeepCode's import/startup fixtures were independently reviewed and corrected;
  tests now guard the actual _stop/_request interfaces rather than invented APIs.
- Vue typecheck and production build passed. The extracted-service API PC fixture
  passed send/stream/stop/supplement/reload/position/permission tests without page
  errors. Desktop screenshots are in output/playwright/assistant-stream.
- Final complete package preflight passed: 53 notice identity, 1552
  learning/assistant, 113 resident backend, 32 convergence, 123 business/lifecycle,
  33 critical guard and 18 transport checks. Two opt-in public network checks
  were skipped; all executed checks passed. Transport publication messages refer
  only to synthetic file:// Git repositories, not the production update remote.

No production Feishu test records were written and no ordinary patch was
published. All native acceptance databases, providers and business writes were
isolated. PC fixture results alone do not prove the separate-process native UI.
An earlier failed probe's abandoned TEMP directory was not recursively deleted
because the execution policy rejected cleanup. Its owned processes had already
exited; it is not a service runtime directory or production business data.

## Resident Iteration: Windows, Transport and Native PC

- `check_openclaw_windows_lifecycle.py` passed a real current-user on-demand
  task launched from a Job-contained parent. The host was outside that Job,
  survived parent exit, and duplicate task launch retained the same host PID.
  Private-console CTRL_C cleanup was 848.6 ms and actual console WM_CLOSE
  cleanup was 51.2 ms, including a real Node gateway. Automatic restart
  respected the manual-stop marker. Only fixture-owned tasks were removed.
- `check_openclaw_update_native.py` passed against a private installed-code copy:
  code replacement, compatible rollback, dependency-failure hold/recovery and
  manual-stop preservation retained conversation ID and attachment bytes.
  This is a real update-guard/process check, not a full distributed ZIP install.
- Windows HTTP client construction measured 702.7-987.9 ms. Main/service local
  transports now reuse pools initialized off-loop. Per-request credentials,
  timeouts and lease authorization remain independent; transport failures are
  not retried. ASGI dispatch still has per-call clients to avoid cookie sharing,
  with their initialization moved off-loop.
- Confirm/refresh atomic store work moved to workers. DeepCode's first revision
  was rejected for post-commit rendering failure and failed-resume reservation
  gaps. Corrections and frozen-response tests passed independent review; an
  accepted operation still schedules once when its response is cancelled.
- Question-material authority/file retrieval remains in the portal, but its
  extraction/cache now belongs to the service. The eighth assistant namespace,
  lighthouse_question_text, is included in first migration. A 69-test material,
  store, real-proxy and import subset passed after this final storage change.
- `check_openclaw_backend_native.py --browser` passed production dist against
  actual Python/Node, original authenticated portal proxy and synthetic APIs:
  private upload/download, model switch with retained history, portal restart
  retaining both PIDs, no message replay, stop/continue and supplementation.
  Native material retrieval preserved its source file and the main database
  retained only the initial seeded assistant document; later writes lived in
  assistant.sqlite3. Human confirmation wrote one isolated business record;
  repeated confirmation did not duplicate it. Desktop images were inspected at
  output/playwright/assistant-resident. No page errors or horizontal overflow.
- Latest native cold question was 69.80 s, retained-gateway question 1.294 s,
  authenticated shutdown 329.5 ms. Cold-period portal P95 was 3.3 ms/max 59.8 ms;
  hot-period max was 4.4 ms. Latest 3-second idle sample was 0.0 percent of one
  core for host and gateway, private memory 70.9/560.3 MiB, both below-normal
  priority. An earlier short gateway sample was 12 percent of one core, so
  these short samples do not prove steady-state CPU or peak memory limits.
- Complete packaging preflight passed before the final material-cache split:
  53 notice identity, 1556 learning/assistant (2 opt-in network skips), resident
  backend groups, 32 convergence, 123 business/lifecycle, 33 guard and 18
  transport tests. New gateway-log, transport-pool and nonblocking-plan tests
  are in the preflight. Publishing messages were fixture file:// Git only.
  The delivery-tree preflight must run again after remaining code changes.

All owned native probe processes exited; production service state and real
Feishu business records were not modified, and no user patch was published.

## Final Iteration Evidence

- Early-start close and duplicate BAT observer were rechecked on Windows after
  the deadline-lock change. Preparation close was 20.7 ms; observer close was
  1191.0 ms and removed the same real host/gateway. Abrupt host termination
  removed its child Job; bounded native recovery and manual-stop preservation
  passed. System-console events are isolated unit fixtures, not a real logoff
  of the user's desktop session.
- The real RemotePatchUpdater ZIP extractor and unchanged original Qt patch
  worker applied a private installed-code replacement. Syntax verification
  failure rolled back and restarted compatible code; shared-dependency failure
  held the service until repair; an explicitly stopped service stayed stopped.
  Interrupted chat IDs were retained and submitted jobs queried their original
  task ID after replacement, without resubmission.
- DeepCode's plan-I/O implementation was independently rejected/corrected for
  repeated cancellation, all-Task shutdown cancellation, worker cancellation,
  and unbounded regression harnesses. Executor-backed futures preserve caller
  context, wait out writes, propagate failures and schedule accepted commits
  once on the original loop. Bounded subprocess tests cover shutdown spinners.
- A full original proxy regression reproduced missing continuation authority
  after a submitted composite plan's first job completed. Status refresh now
  continues only the persisted approved plan; unconfirmed, wrong-owner and
  revoked-scope plans do not execute, and repeated refresh does not resubmit.
  The final 51-test proxy/plan/lifetime subset passed independently.
- Deleted attachment tombstones migrate without copying/resurrecting missing
  bytes. Active missing/corrupt/escaping files still fail safely. Same-host
  portable relocation, ciphertext, IDs, backup and business-DB isolation remain
  covered by the store suite and private installed-process probe.
- Actual full-source patch construction exposed long imported-skill paths.
  The packager now stores all 355 approved resources in workbuddy.zip, preserving
  each byte and registry name/reference. The reader never extracts it and
  prefers it over old physical resources. Real ZIP member lengths, metadata
  hashes, resource equality and runtime/development data exclusion passed.
  Corrupt, oversized, duplicate, linked and unregistered resources are rejected.
- Production Vue typecheck/build and actual Python/Node PC fixture passed.
  Latest cold/hot query times were 60.180/1.079 s; portal cold P95/max were
  3.1/6.7 ms and authenticated shutdown was 334.3 ms. PC upload/download,
  context/model switching, portal restart/no replay and stop/supplement passed;
  latest screenshots were visually inspected in output/playwright/assistant-resident.
- Final two-slot native measurement: 45.1-second idle CPU was 0.03 percent of
  one core for host and 0.17/0.21 for gateways; combined peak private memory was
  952.8 MiB, all below-normal priority. Qt timer P95/max were 17.0/27.9 ms.
  These are synthetic workload measurements, not an upstream-model SLA or a
  guarantee for arbitrary machine load. Owned TCP listeners were loopback only;
  the pinned runtime's LAN discovery is disabled and no owned mDNS socket existed.
- Complete delivery-tree preflight passed after bundle/continuation/cancellation
  fixes: 53 notice identity, 1566 learning/assistant (2 opt-in public-network
  skips), all resident groups, 32 convergence, 123 business/lifecycle, 33 guard
  and 18 transport tests. Source compilation covered 114 files. Transport
  publication fixtures used local file:// Git, not production publishing.

No real Feishu business test records were written. No production patch was
published. All private probe processes/tasks were cleaned up. Ordinary project
state and the existing production scheduled task were not removed.

## Completion Audit

| Requirement | Authoritative evidence |
| --- | --- |
| Independent implementation/storage; main only frontend/auth/bridge | Service module tree, main constructor/writer search, real proxy main-DB count |
| One Python/BAT entry, current-user task, no boot trigger/SYSTEM | BAT, launcher policy tests, actual task outside parent Job and singleton probe |
| Main restart reuse and stale lease revocation | Real PC portal restart with retained host/gateway PIDs; proxy/client revoke tests |
| Manual console close <=5 s, bounded crash recovery | Real window/observer/preparation/Job probes and launcher/client/early-exit tests |
| Assistant-only recoverable migration | Store/backup/relocation/corruption/tombstone suite and installed-process data proof |
| Same API/SSE/stop/supplement/model/files UI | Original proxy tests and production-dist native PC fixture |
| Original permissions, no direct Bitable mutation, PII rules | Agent boundaries, original native catalog, business matrix and proxy role tests |
| Work-order execution excluded, Qt events preserved, learning read-only | Native catalog restrictions, interactive coverage and 1566-test regression group |
| Main updater only, pause/shared deps/rollback/manual stop | Original Qt worker real ZIP fixture plus update guard/transport suites |
| Approved assets present; private/runtime/dev data excluded | Real source selection and actual full patch ZIP/hash/resource comparison |
| Current/one preceding generation only | Import aliases, previous account-folder relocation, legacy source migration; no second implementation |
| Two slots, serialized cold starts, priority/reuse/bounded logs | Native resource/network/Qt probe and runtime/gateway-log suites |

## Final Gate Passed

- The complete preflight passed again after the final LAN-discovery disable
  flag: all executed groups passed, 1566 learning/assistant tests included two
  opt-in public-network skips. The focused runtime/artifact tests and native
  network/resource/Qt probe also passed with the final flag. No runtime edits
  followed this gate.

The independent-backend implementation and specified isolated acceptance gates
are complete. Production publishing was not performed by these probes; a
normal user release must still run package_portable.py and its publication
verification rather than reuse any private acceptance artifact.

## 2026-10-06 Performance Follow-Up

This section supersedes older topology/performance claims above. The current
runtime uses one shared gateway with separate account agents and conversations.

- Actual user-assisted Qt drag profiling collected 343 active samples; 193
  landed in the Python mouseMoveEvent/self.move path. Native Qt system movement
  replaces that path, retaining the unsupported-platform fallback. The user
  confirmed a large improvement after restarting, with some residual drag lag.
- Portal sampling found repeated identical certificate-bundle loads and
  historical per-record rereads. Verified TLS contexts are reused under the
  same CA policy; inactive change tasks and non-due SOP reminders skip redundant
  reads. Due reminders still reread under the original lock before processing.
- Empty outbox leases and counts no longer acquire a write transaction unless
  there is eligible work or an expired lease. Queue details are read once.
  SSE statistics and full-health SQLite reads run outside the async event loop;
  the lightweight connectivity probe still does not depend on SQLite.
- The assistant avoids duplicate conversation polling during healthy text
  streaming, restores scroll position on reopen, clears stale render timers,
  and uses compositor transforms during panel dragging. Existing permissions,
  explicit business confirmation, fallback polling and account isolation remain.
- Generic public follow-up normalization uses only the user's words, rejects
  private/business data before network access, and resets search limits per
  turn. Cached searches retain their original source references. A real-model
  isolated probe returned public comparison evidence in 9.38 seconds; this is
  one sample, not a model/network SLA. Weather display uses Chinese descriptions.
- The 284-test performance/assistant/backend group passed with two opt-in
  public-network skips. After the final query-label cleanup, 80 focused tests,
  two public follow-up/budget tests and 27 API/queue regression tests passed.
  The API fixture was updated to accept and assert existing ledger-device IDs;
  no repair runtime change was made for that fixture mismatch.
- Vue typecheck and production build passed. The built PC fixture retained both
  bottom-following and a reading position 720.7 pixels above the end after
  close/reopen; panel drag commits without a stale transform. Syntax and scoped
  diff checks passed. The isolated preview does not use production business data.

No real Feishu business test record was written or production patch published.
The extended whole-business regression was interrupted for clean profiling and
is not a passed release gate. Full packaging preflight must still run before
publishing. These changes do not prove zero lag on arbitrary system load; the
remaining small user-observed drag slowdown has not been assigned to a driver
or unrelated desktop process. New Python changes need the next normal restart.

### Backup, Backfill and Home Statistics

- Live profiling subsequently found an online SQLite backup repeatedly
  restarting its pages while other connections committed. The 408.5 MiB source
  had been copying for almost half an hour. A pinned read-only WAL snapshot now
  bounds the copy without blocking writers. The concurrent-write regression
  fails on the former implementation and passes on the fix. A private read-only
  copy of the real database completed in 23.609 seconds; integrity checking
  returned `ok` after 38.24 seconds total. The private copy was removed.
- An aged terminal failed event-to-repair task was mistaken for newly queued
  work, because INSERT OR IGNORE returned its existing ID. This continuously
  woke the worker and reread whole snapshots. The backfill now checks failed
  tasks alongside pending, leased and completed tasks. Startup and explicit
  retry paths remain unchanged. The former implementation reproduces the
  regression; the fix retains the failed task and queues fresh events once.
- The busy scan held the local database lock needed by home statistics. After
  the user's restart, the actual authenticated production homepage showed all
  four metric pairs again. Returning from the maintenance building selector
  displayed those values immediately (386 ms for the browser action in one
  observation), without placeholders or waiting for a new overview response.
  Values may subsequently change with ordinary production synchronization.
- The repaired queue resumed real missing-project backfill. Read-only inspection
  distinguished advancing unique event tasks from a failed-task spin. This is
  ordinary application recovery, not synthetic production test-data insertion.
- Learning list reads now select the same payload, revision and dirty fields
  in one ordered query rather than rereading each row. Terminal and deferred
  notifications no longer reread settings before being skipped. Tests preserve
  ordering, revisions, dirty flags, caller-owned connections and notification
  behavior. The final combined performance/learning group passed 89 tests.
- Signature polling retains in-flight ownership across close/reopen, avoiding
  overlapping requests and stale restart timers. The existing request-reliability
  check, read-cache check, Vue typecheck and production build passed.

No statistics definition, task-assignment rule or upload business rule was
changed. Restart checks defer while work is active; the user has authorized
developer-managed restarts without another manual confirmation.

### Live Assistant and Business Read Acceptance

- On the actual authenticated PC page, querying currently ongoing notices across
  authorized buildings returned three unfinished notices, all maintenance,
  matching the displayed workbench. It did not substitute a cross-module work
  summary. The persisted run completed in 3.146 seconds.
- Asking for today's Nantong weather automatically used the public weather
  evidence and returned a concise Chinese answer. The persisted run completed
  in 0.860 seconds. These are individual warm-run measurements, including any
  valid source reuse, not promises about external-model or network latency.
- Existing conversation history and Markdown tables/bold formatting restored
  on the production page. The submitted questions appeared in the conversation
  and the composer cleared; no business operation was requested or confirmed.
- Seventeen isolated business-read regressions passed: home overview counts,
  authorized preloads/filtering, change-task seeding, queue leasing/recovery,
  and verified daily backups. Test data and acceptance logs are under output;
  no synthetic records were sent to business tables. An initial temporary-log
  cleanup failed on Windows; rerunning with retained isolated logs exited zero.

The live portal continues its legitimate missing-project recovery. A restart
has been deferred while writes remain active. The two final learning-reader
optimizations are verified by the 89-test group but still await live activation.

### 2026-10-07 Final Runtime Activation

This section closes the activation item above.

- After the recovery queue reached 344 completed tasks and one retained terminal
  failure, three separate read-only checks found no pending or leased work.
  The old backend exited normally through its existing loopback shutdown API.
  The new backend was launched with the existing desktop authorization policy
  and the same Qt parent. Qt PID 103584 remained alive; backend PID 91880 loaded
  the final learning-reader changes. No Qt window or unsaved form was forcibly
  closed. The reconnect page was refreshed by the developer without user action.
- Three eight-second quiet samples measured Qt at 0.2-0.8 percent of one core,
  the assistant at 0-0.6 percent and the shared gateway at 0.2-1.0 percent.
  Portal CPU was 10.7-17.0 percent of one core, reading 2.37-8.63 MiB per sample,
  compared with the earlier 196.1 percent and 302.97 MiB. No tests or builds ran
  during this measurement. These are observations, not arbitrary-load guarantees.
- Final profiling found normal relay reconciliation and empty queue checks,
  not the former backup restart or failed-task wake loop. Relay wire messages,
  command polling cadence and work-order execution rules were left unchanged.
- The shared gateway became ready and loaded 51 approved skills. Its cold
  preparation still takes time in the background; ordinary business pages do
  not wait for it. Existing account conversations and four model choices restored.
- On the E-building maintenance page, an unqualified administrator question
  returned all authorized buildings' three ongoing maintenance notices in
  4.574 seconds. Supplementing "only E" narrowed the result to two in 2.240
  seconds. Both results matched the displayed building overview. Returning to
  the home page retained numeric statistics without placeholders.
- Snapshot counts changed during legitimate recovery and source refresh.
  Inspection confirmed the original monthly event candidate rule includes
  last-modified time as well as occurrence/progress/recovery/end times. That
  rule was not changed; a refreshed monthly snapshot is not a count of newly
  occurring events. Exact occurrence-date assistant queries retain their own
  separate date filter.

| Goal requirement | Current acceptance evidence |
| --- | --- |
| Diagnose and improve whole-PC/Qt lag | Actual drag profiles, native movement tests, concurrent-write backup regression, failed-task reproduction, user's improved drag feedback, final process measurements |
| Fast frontend and smooth assistant interaction | Built PC fixture, request/read-cache checks, real home return, restored history/Markdown, compositor drag and guarded close/reopen behavior |
| Accurate and fast business/general answers | Exact-query and permission regressions, real all-building/E-only answers, Chinese weather tool answer, public comparison probe and follow-up/budget guards |
| Common tools used automatically | Current 51-skill gateway readiness, real weather evidence use, public-source and imported-skill safety tests |
| Preserve business logic and data safety | Scoped business regressions, unchanged monthly criteria/relay protocol, isolated 89+17 test groups, no synthetic cloud business writes or production publishing |

The current goal's implementation and runtime acceptance are complete. Full
packaging preflight remains a separate release gate before publishing a user
patch; this audit does not claim every unrelated business integration was
exercised against production, or that external model/network latency is fixed.
