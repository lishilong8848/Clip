# ClipFlow LAN Portal Frontend

This is the Vue 3 migration workspace for the LAN portal.

The production portal serves the Vue dist by default when the dist contains
`clipflow-frontend-ready.json` with:

```json
{"app":"clipflow-lan-portal","productionReady":true}
```

This marker prevents the migration placeholder from being exposed to field
users. `CLIPFLOW_FRONTEND_LEGACY=1` forces the legacy static page for rollback.
Do not commit `node_modules` or user runtime data.

Rollback switches:

- `CLIPFLOW_FRONTEND_LEGACY=1`: keep serving the legacy static page.
- `CLIPFLOW_LEGACY_QT_WIDGET_LIST=1`: keep the old Qt widget list.
