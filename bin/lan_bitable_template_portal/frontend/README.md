# ClipFlow LAN Portal Frontend

This is the Vue 3 migration workspace for the LAN portal.

The production portal still serves `static/index.html` by default. Build outputs from
this workspace should be copied into the portal static directory only after the Vue
version reaches parity. Do not commit `node_modules` or user runtime data.

Rollback switches:

- `CLIPFLOW_FRONTEND_LEGACY=1`: keep serving the legacy static page.
- `CLIPFLOW_LEGACY_QT_WIDGET_LIST=1`: keep the old Qt widget list.
