# Workspace V2 Design Contract

## VIPBoard Integration

The local `vipboard-charts` skill supplies visual references, not production business code. `vipboard.ts` adapts bar, horizontal comparison, line, area, donut and pie patterns to the existing ECharts module; `MotionChart.vue` owns lifecycle, loading/error/zero states, resizing and drill-down. No second chart runtime or raw demo HTML is loaded.

Personal workspace templates are optional, permission-filtered and stored by the existing per-user layout contract. Water charts compare only water usage in tonnes; learning charts use claimed paper counts; event trends retain the existing date-range query and detail records. Unknown values are not plotted as zero. Native Vue business tables/forms receive scoped presentation rules in `vipboard.css`; their controls, handlers and backend contracts remain intact.

The desktop sidebar keeps the existing module list and permissions, adds local search and keyboard selection, and retains independent scrolling. Narrow desktop expansion must not hide labels. Mobile retains the bottom navigation.

Checks: `check:vipboard`, the workspace layout/customization suite, event/cabinet tests, typecheck and the independent V2 production build. The skill also has `scripts/check-runtime.mjs` for reusable library safety/lifecycle regressions.

Source: UI/UX Pro Max v2.15.0, verified local searches on 2026-10-12.
Motion reference: linyi-ui/ui-motion-skill, pinned commit f29be3bd659f8a6bab7fdfe6e8d589e40ccf004d. Reviewed SKILL and references; installed as a development-only skill. Apply Submit Morph, Jelly Toggle and Streaming Reply only to the presentation, retaining existing validation, confirmations and checkbox multi-selection semantics.

- `enterprise admin dashboard --design-system --density 8 --variance 3`: enterprise hierarchy, low-cost motion, clear status and source age.
- `mobile navigation responsive --domain ux`: mobile-first layout, contained tables, no page overflow.
- `category comparison bar --domain chart`: labeled category/value comparisons, no invented health scores or mixed-unit totals.
- `component library forms responsive --stack vue`: Vue SFCs and established form components.

The direction is a white VNET + Lighthouse header, neutral pale surfaces and blue primary actions, with green success, amber attention and red error accents. The skill's Minimalism & Swiss enterprise style fits this workflow; its marketing hero/CTA suggestions do not and are excluded. System Chinese fonts avoid remote downloads. No promotional hero or invented capability score is introduced.

## Components

New controls use Naive UI 2.45.3: NButton, NMenu, NSelect, NDropdown, NTabs, NDataTable, NTag, NProgress, NAlert, NResult, NDescriptions and NSpin. All iconography is lucide-vue-next. Theme tokens live in theme.ts; layouts in workspace.css.

Existing Vue business components are imported read-only. Vite's V2-only adapter maps browser route reads and navigation to the independent entry without changing their source or backend contracts. NoticeWorkspace and WorkOrderWorkspace are new Vue surfaces, not embedded legacy documents. Notices read the existing detail endpoint as inert field metadata and render Naive UI fields; they never mount returned HTML, CSS or scripts. Signed work orders use the existing session and step APIs and retain token, role, photo and version checks. Standalone legacy pages remain unchanged.

The notice screen displays plans and ongoing items side by side on desktop, with independent pagination and inline start/update/end actions. Mobile uses a compact lane switch. Building selection shares the filter toolbar; there is no repeated statistics panel or legacy document iframe. Editing has a fixed action footer, date controls, original SOP selector and uploads. Unknown write results retain the operation ID for original-task recovery. A preview does not submit. Navigation guards compose across independent forms. Run `check:workspace-notices` for real backend-rendered field fixtures, six notice categories, signed work-order interactions and viewport checks; these tests mock network writes and do not prove real Feishu delivery.

The notice editor switches between filling and review inside one modal. Update/end put current progress and missing fields first; original facts remain editable in an expandable section. The preview is display-only and is checked against all six original web preview formats. Existing photos, SOP links and upload recovery remain available. Only real task results drive completion feedback.

The compact toolbar exposes typed manual creation and pasted-notice parsing, SOP template CRUD (attachments, repeat rules and type conversion), target binding, repair/event association, privileged change confirmations and undo. Pasted text uses the existing HTML parser endpoint through a V2-only build adaptation of the shared transport; returned scripts/styles are never mounted. The old client source is unchanged. Source synchronization is an explicit action separate from page refresh. Destructive actions and association replacement require confirmation; server permissions and versions remain authoritative.

Handling records are the current user's known jobs from this browser session (at most 100 over seven days), not an invented cross-computer audit history. The batch endpoint authorizes every job before display. Unknown write responses keep the original payload and operation ID. SOP creation has no server idempotency API, so an unknown save result disables direct re-submission until the user checks the existing list. These limits must not be hidden by automatic retries.

Run `check:workspace-notices`, including the notice-tools browser suite, for six real-renderer fixtures, metadata safety, original API request shapes, one-window preview, SOP/association/confirmation workflows, user isolation and responsive layout. Network writes are isolated fixtures, not evidence of live Feishu delivery. Field metadata remains coupled to the existing detail response, so backend form changes must rerun these checks.

PC: compact header, 224px navigation, wide task table, authorized counts, supporting category comparison. Mobile: 56px header, two-column counts, single-column tasks, four bottom navigation entries, scroll-contained data tables. No continuous dashboard animation. Reduced-motion is honored.

## Roles and Feedback

- Navigation is flat: one notice workspace entry retains all six independent permission checks. The home dashboard has no repeated shortcut grid or learning-entry panel; module headings open the corresponding business surface. The mobile Functions page is the sole grouped directory. The assistant keeps its persistent launcher instead of a second directory entry.
- Event entry opens the authorized scope's list immediately; risk, drill and cabinet entry preserve direct building views. Scope choice survives in-app navigation only for the same authenticated identity and still-authorized scope. Guest views have no unrelated sidebar or bottom navigation.
- UI/UX Pro Max's verified navigation/back guidance and ui-motion's short transform/opacity feedback guide the compact function controls. Links retain visible focus, mobile targets are at least 44px, and reduced motion disables decorative transitions. No library or background service is added for these refinements.
- L0/L1 (including H duty) use the global overview before any duty/category rule. Supervisors prioritize events and notice schedules; engineers prioritize events, repair and convergence. Duty accounts have one-click notice/water/drill/risk tabs. Personal accounts show their own learning, never building tasks labelled as personal work orders.
- Read only existing APIs. Missing personal work-order inventory is stated explicitly; signed execution links stay valid. Convergence reads persisted repair/change results, does not connect to VPN at dashboard startup, and preserves D/E exclusions.
- Reads have three concurrent slots, invisible panels load on approach, stale requests are cancelled, and module errors remain unknown rather than zero.
- Buttons: 300ms ease-in-out visual contraction, minimum 1.2s processing feedback, success only after the observed request completes, then restore after 1s. Errors never show a green check. Layout allocation stays stable.
- Checkboxes keep multi-select semantics and indeterminate/disabled states, with a 350ms jelly-switch appearance. Native and Naive UI controls both support keyboard interaction.
- Only V2 replaces the reply presentation: original sanitized Markdown, new text in 2-4 character groups, 3-dot pending indicator, smooth growth, streaming caret and scroll pause on manual upward movement. Historical messages do not replay their animations.

## Boundaries

No backend or data migration. No frontend-created grants. No direct Feishu requests. No new event-send or work-order authority. The only changed legacy component is AppTopbar, adding the version switch. Preferences contain only the version string keyed by authenticated identity in this site's browser storage.
