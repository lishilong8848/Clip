# Bloub Bot

Source: https://github.com/jeremy-prt/bloub (MIT).
Vanilla SVG renderer supplied in the user's local bloub-bot package.
Only runtime source files are included; no node_modules or demo files.

ClipFlow changes: bounded frame rate, explicit animation suspension, reused eye
paths, and click reactions returning to idle even without sequence playback.
Dragging and spring-back use the supplied renderer. ClipFlow adds viewport
bounds, account-isolated settings and retained positions, while keeping the
existing assistant panel's independent drag behavior.
