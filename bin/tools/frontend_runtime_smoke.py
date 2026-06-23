# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parents[1]
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from clipflow_backend.main import FastAPIPortalController, _wait_until_listening  # noqa: E402
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME, PortalAuthManager  # noqa: E402
from lan_bitable_template_portal.portal_service import SCOPE_OPTIONS  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime, find_available_port  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class _SmokePortalService:
    _last_loaded_at = "smoke"
    _last_loaded_ts = 0.0
    _load_warnings: list[str] = []

    def _normalize_scope(self, scope: str) -> str:
        text = str(scope or "").strip().upper()
        if text in {"ALL", "CAMPUS", "110"}:
            return text
        for code in ("A", "B", "C", "D", "E", "H"):
            if code in text:
                return code
        return "ALL"

    def _scope_matches_item(self, scope: str, item: dict) -> bool:
        normalized = self._normalize_scope(scope)
        if normalized == "ALL":
            return True
        codes = [
            self._normalize_scope(code)
            for code in (item.get("building_codes") or [])
            if self._normalize_scope(code) not in {"", "ALL"}
        ]
        if not codes:
            return True
        if normalized == "CAMPUS":
            return len(set(codes)) >= 2
        return len(set(codes)) == 1 and codes[0] == normalized

    def get_scope_overview(
        self,
        *,
        ongoing_items: list[dict] | None = None,
        scopes: list[str] | None = None,
        include_prepared: bool = False,
    ) -> dict:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        scope_values = [str(scope or "").strip() for scope in (scopes or []) if str(scope or "").strip()]
        if not scope_values:
            scope_values = ["A", "B"]
        items: dict[str, dict] = {}
        for scope in scope_values:
            items[scope] = {
                "scope": scope,
                "maintenance_pending": 1,
                "change_pending": 1 if scope == "A" else 0,
                "repair_pending": 0,
                "maintenance_ongoing": 0,
                "change_ongoing": 0,
                "repair_ongoing": 0,
                "closed_today": 0,
                "last_loaded_at": now,
                "warnings": [],
            }
        return {
            "items": items,
            "scopes": items,
            "last_loaded_at": now,
            "warnings": [],
            "source_snapshot_ready": True,
        }

    def get_handover_links(self) -> dict:
        return {
            "scope_options": SCOPE_OPTIONS,
            "links": {
                "A": "https://example.invalid/a",
                "B": "https://example.invalid/b",
            }
        }

    def query_records(
        self,
        *,
        month: str = "",
        specialty: str = "",
        scope: str = "ALL",
        ongoing_items: list[dict] | None = None,
    ) -> dict:
        today = time.strftime("%Y-%m-%d")
        records = [
            {
                "record_id": "src-maint-a-001",
                "source_record_id": "src-maint-a-001",
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "building_codes": ["A"],
                "source_progress": "未开始",
                "maintenance_cycle": "每月",
                "display_fields": {
                    "楼栋": "A楼",
                    "维护总项": "冷机月度巡检",
                    "维护周期": "每月",
                    "维护实施状态": "未开始",
                    "专业类别": "暖通",
                },
            },
            {
                "record_id": "src-change-a-001",
                "source_record_id": "src-change-a-001",
                "work_type": "change",
                "notice_type": "变更通告",
                "building_codes": ["A"],
                "source_progress": "未开始",
                "display_fields": {
                    "变更简述": "A楼网络设备变更测试",
                    "变更楼栋": "A楼",
                    "变更进度": "未开始",
                    "专业": "网络",
                    "变更等级（阿里）": "I3",
                },
            },
        ]
        ongoing = [
            {
                "active_item_id": "active-change-a-001",
                "record_id": "target-change-a-001",
                "target_record_id": "target-change-a-001",
                "source_record_id": "src-change-active-a-001",
                "work_type": "change",
                "notice_type": "变更通告",
                "title": "A楼UPS旁路切换变更测试",
                "building": "A楼",
                "building_codes": ["A"],
                "specialty": "供配电",
                "level": "I3",
                "start_time": f"{today}T09:30",
                "end_time": f"{today}T18:30",
                "location": "A楼配电间",
                "content": "测试测试测试",
                "reason": "测试测试测试",
                "impact": "测试测试测试",
                "progress": "测试测试测试",
                "status": "已开始",
                "uploaded": True,
            }
        ]
        return {
            "scope": scope,
            "records": records,
            "ongoing": ongoing,
            "zhihang_change_records": [
                {
                    "record_id": "zhihang-a-001",
                    "title": "A楼智航侧变更测试",
                    "building": "A楼",
                    "progress": "进行中",
                }
            ],
            "daily_summary": {
                "date": today,
                "items": [],
                "stats": {"started": 1, "updated": 0, "ended": 0, "ongoing": 1},
            },
            "defaults": {
                "impact": "对业务无影响",
                "progress": "准备工作已完成",
            },
            "warnings": [],
            "last_loaded_at": "smoke",
            "source_snapshot_ready": True,
        }

    def list_available_notice_undos(
        self,
        *,
        scope: str = "ALL",
        action_type: str = "",
        since_seconds: float = 0,
    ) -> list[dict]:
        return [
            {
                "undo_id": "undo-smoke-delete-a-001",
                "undo_action_type": "delete",
                "work_type": "change",
                "notice_type": "变更通告",
                "title": "A楼UPS旁路切换变更测试",
                "building": "A楼",
                "building_codes": ["A"],
                "undo_created_at": time.time(),
                "active_item_id": "active-change-a-001",
                "target_record_id": "target-change-a-001",
                "undo_label": "撤销删除",
            }
        ]


def _build_playwright_script(url: str, session_id: str) -> str:
    no_scope_session_id = f"{session_id}-no-scope"
    payload = {
        "url": url,
        "session_id": session_id,
        "no_scope_session_id": no_scope_session_id,
        "cookie_name": AUTH_COOKIE_NAME,
    }
    return textwrap.dedent(
        f"""
        const {{ chromium }} = require('playwright');
        const cfg = {json.dumps(payload, ensure_ascii=False)};

        (async () => {{
          const browser = await chromium.launch({{ headless: true }});
          const errors = [];
          const failedResponses = [];

          function attachDiagnostics(targetPage, label) {{
            targetPage.on('console', msg => {{
              if (msg.type() === 'error') errors.push(`${{label}}: ${{msg.text()}}`);
            }});
            targetPage.on('pageerror', err => errors.push(`${{label}}: ${{String(err)}}`));
            targetPage.on('response', response => {{
              if (response.status() >= 400) {{
                failedResponses.push(`${{label}}: ${{response.status()}} ${{response.url()}}`);
              }}
            }});
          }}

          async function assertLayout(targetPage, stage) {{
            const issues = await targetPage.evaluate(() => {{
              const result = [];
              const rectOf = selector => {{
                const node = document.querySelector(selector);
                if (!node) return null;
                const rect = node.getBoundingClientRect();
                return {{
                  left: rect.left,
                  right: rect.right,
                  top: rect.top,
                  bottom: rect.bottom,
                  width: rect.width,
                  height: rect.height,
                }};
              }};
              const intersects = (a, b) => {{
                if (!a || !b) return false;
                return !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);
              }};
              if (document.documentElement.scrollWidth > window.innerWidth + 4) {{
                result.push(`horizontal overflow ${{document.documentElement.scrollWidth}}>${{window.innerWidth}}`);
              }}
              const titleRect = rectOf('.brand h1');
              if (!titleRect || titleRect.width < 240) result.push('brand title missing or too narrow');
              if (titleRect && titleRect.height > 46) result.push(`brand title wrapped: ${{Math.round(titleRect.height)}}px`);
              const brandRect = rectOf('.brand');
              const actionsRect = rectOf('.topbar-actions');
              if (intersects(brandRect, actionsRect)) result.push('topbar brand overlaps actions');
              const summaryCards = Array.from(document.querySelectorAll('.summary-strip article'));
              if (summaryCards.length) {{
                const narrow = summaryCards
                  .map((node, index) => [index + 1, node.getBoundingClientRect().width])
                  .filter(([, width]) => width < 190);
                if (narrow.length) result.push(`summary cards too narrow: ${{JSON.stringify(narrow)}}`);
              }}
              const panels = Array.from(document.querySelectorAll('.workspace > .panel'));
              if (panels.length) {{
                if (panels.length < 3) result.push(`workspace panel count ${{panels.length}}`);
                const narrow = panels
                  .map((node, index) => [index + 1, node.getBoundingClientRect().width])
                  .filter(([, width]) => width < 250);
                if (narrow.length) result.push(`workspace panels too narrow: ${{JSON.stringify(narrow)}}`);
              }}
              return result;
            }});
            if (issues.length) throw new Error(`layout issues at ${{stage}}: ${{issues.join('; ')}}`);
          }}
          async function assertVnetSkin(targetPage, stage) {{
            const issues = await targetPage.evaluate(() => {{
              const result = [];
              const styleText = (node, prop) => node ? String(getComputedStyle(node)[prop] || '') : '';
              const backgroundText = (node) => {{
                const image = styleText(node, 'backgroundImage');
                if (image && image !== 'none') return image;
                return styleText(node, 'backgroundColor');
              }};
              const topbar = document.querySelector('.app-topbar, .topbar');
              const logo = document.querySelector('.brand-logo');
              const card = document.querySelector('.center-state, .feature-card, .panel, .permission-row, .match-layout, .module-card, .home-metrics article');
              const primaryButton = document.querySelector('.btn.blue, button.primary, a.primary, .module-actions button.primary, .scope-actions .primary');
              const topbarBackground = backgroundText(topbar);
              const cardRadius = styleText(card, 'borderRadius');
              const cardBackground = backgroundText(card);
              const primaryBackground = backgroundText(primaryButton);
              if (!logo) result.push('official logo missing');
              if (!topbarBackground.includes('gradient')) result.push(`topbar is not gradient: ${{topbarBackground}}`);
              if (!card) result.push('main white card missing');
              const numericRadius = Number.parseFloat(cardRadius || '0');
              if (card && (!Number.isFinite(numericRadius) || numericRadius < 12 || numericRadius > 28)) {{
                result.push(`card radius not VNET-like: ${{cardRadius}}`);
              }}
              if (card && !/(255, 255, 255|#fff|white)/i.test(cardBackground)) {{
                result.push(`card is not white/light: ${{cardBackground}}`);
              }}
              if (primaryButton && !primaryBackground.includes('gradient')) {{
                result.push(`primary button is not gradient: ${{primaryBackground}}`);
              }}
              return result;
            }});
            if (issues.length) throw new Error(`VNET skin issues at ${{stage}}: ${{issues.join('; ')}}`);
          }}
          async function assertHeaderSubtitle(targetPage, expected, stage) {{
            const headerText = await targetPage.locator('.brand').innerText({{ timeout: 10000 }});
            if (!headerText.includes(expected)) {{
              throw new Error(`header subtitle mismatch at ${{stage}}: expected "${{expected}}", got "${{headerText}}"`);
            }}
            if (/HTTP\\s+\\d+/i.test(headerText)) {{
              throw new Error(`header exposes technical HTTP status at ${{stage}}: ${{headerText}}`);
            }}
          }}
          async function eventSourceUrls(targetPage) {{
            return await targetPage.evaluate(() => Array.from(window.__clipflowActiveEventSourceUrls || []));
          }}

          const unauthContext = await browser.newContext();
          const unauthPage = await unauthContext.newPage();
          attachDiagnostics(unauthPage, 'unauth');
          const unauthResponse = await unauthPage.goto(cfg.url, {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          if (!unauthResponse || !unauthResponse.ok()) {{
            throw new Error(`unauth page load failed: ${{unauthResponse && unauthResponse.status()}}`);
          }}
          await unauthPage.waitForSelector('text=南通基地-运维灯塔工作台', {{ timeout: 10000 }});
          await unauthPage.waitForSelector('text=飞书扫码登录', {{ timeout: 10000 }});
          const unauthText = await unauthPage.locator('body').innerText({{ timeout: 10000 }});
          if (!unauthText.includes('请先使用飞书登录')) throw new Error('missing unauth login prompt');
          await assertHeaderSubtitle(unauthPage, '功能选择 · 请先登录', 'unauth');
          await assertLayout(unauthPage, 'unauth');
          await assertVnetSkin(unauthPage, 'unauth');
          await unauthContext.close();

          const noScopeContext = await browser.newContext();
          await noScopeContext.addCookies([{{
            name: cfg.cookie_name,
            value: cfg.no_scope_session_id,
            domain: '127.0.0.1',
            path: '/',
          }}]);
          const noScopePage = await noScopeContext.newPage();
          attachDiagnostics(noScopePage, 'no-scope');
          const noScopeResponse = await noScopePage.goto(cfg.url, {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          if (!noScopeResponse || !noScopeResponse.ok()) {{
            throw new Error(`no-scope page load failed: ${{noScopeResponse && noScopeResponse.status()}}`);
          }}
          await noScopePage.waitForSelector('text=当前账号暂无门户权限', {{ timeout: 10000 }});
          await noScopePage.waitForSelector('text=提交给管理员', {{ timeout: 10000 }});
          await assertHeaderSubtitle(noScopePage, '功能选择 · 申请访问权限', 'no-scope');
          const scopePillCount = await noScopePage.locator('.scope-pill').count();
          if (scopePillCount < 8) throw new Error(`permission request scope pills too few: ${{scopePillCount}}`);
          await noScopePage.locator('.scope-pill').filter({{ hasText: 'A楼' }}).click();
          const selectedPillText = await noScopePage.locator('.scope-pill.selected').innerText({{ timeout: 10000 }});
          if (!selectedPillText.includes('A楼')) throw new Error(`permission request pill selection failed: ${{selectedPillText}}`);
          await assertLayout(noScopePage, 'no-scope');
          await assertVnetSkin(noScopePage, 'no-scope');
          await noScopeContext.close();

          const context = await browser.newContext();
          await context.addInitScript(() => {{
            const NativeEventSource = window.EventSource;
            if (!NativeEventSource || window.__clipflowEventSourcePatched) return;
            window.__clipflowEventSourceUrls = [];
            window.__clipflowActiveEventSourceUrls = [];
            window.EventSource = function(url, options) {{
              const urlText = String(url);
              window.__clipflowEventSourceUrls.push(urlText);
              window.__clipflowActiveEventSourceUrls.push(urlText);
              const source = new NativeEventSource(url, options);
              const nativeClose = source.close.bind(source);
              source.close = function() {{
                const index = window.__clipflowActiveEventSourceUrls.indexOf(urlText);
                if (index >= 0) window.__clipflowActiveEventSourceUrls.splice(index, 1);
                return nativeClose();
              }};
              return source;
            }};
            window.EventSource.prototype = NativeEventSource.prototype;
            window.__clipflowEventSourcePatched = true;
          }});
          await context.addCookies([{{
            name: cfg.cookie_name,
            value: cfg.session_id,
            domain: '127.0.0.1',
            path: '/',
          }}]);
          const page = await context.newPage();
          attachDiagnostics(page, 'auth');
          const response = await page.goto(cfg.url, {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          if (!response || !response.ok()) {{
            throw new Error(`page load failed: ${{response && response.status()}}`);
          }}
          await page.waitForSelector('text=南通基地-运维灯塔工作台', {{ timeout: 10000 }});
          await page.waitForSelector('text=功能选择', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, '功能选择 · 请选择功能', 'home');
          await assertVnetSkin(page, 'home');
          const bodyText = await page.locator('body').innerText({{ timeout: 10000 }});
          const required = ['功能选择', '事件管理', '维护管理', '变更管理', '其他工具'];
          for (const marker of required) {{
            if (!bodyText.includes(marker)) throw new Error(`missing marker: ${{marker}}`);
          }}
          const forbidden = ['Vue migration workspace', '当前生产页面仍由 legacy index.html 提供'];
          for (const marker of forbidden) {{
            if (bodyText.includes(marker)) throw new Error(`legacy marker visible: ${{marker}}`);
          }}
          await assertLayout(page, 'home');
          await page.locator('.module-card.slate').getByRole('button', {{ name: '交接班', exact: true }}).click();
          try {{
            await page.waitForFunction(() => document.body.innerText.includes('选择楼栋打开交接班审核页'), null, {{ timeout: 10000 }});
          }} catch (err) {{
            const currentText = await page.locator('body').innerText({{ timeout: 10000 }});
            throw new Error(`handover entry did not open. Current page text: ${{currentText.slice(0, 800)}}`);
          }}
          const handoverScopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await handoverScopeCard.getByRole('link', {{ name: '打开审核页' }}).waitFor({{ timeout: 10000 }});
          const handoverHref = await handoverScopeCard.getByRole('link', {{ name: '打开审核页' }}).getAttribute('href');
          if (handoverHref !== 'https://example.invalid/a') {{
            throw new Error(`handover link href mismatch: ${{handoverHref}}`);
          }}
          await assertLayout(page, 'handover-feature');
          await page.getByRole('button', {{ name: '返回功能选择' }}).click();
          await page.waitForSelector('text=业务工作台', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, '功能选择 · 请选择功能', 'home-after-handover');
          await page.getByRole('button', {{ name: '进入维护管理', exact: true }}).click();
          await page.waitForSelector('text=选择楼栋进入维护管理', {{ timeout: 10000 }});
          const scopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await scopeCard.getByRole('button', {{ name: '进入维护管理' }}).click();
          await page.waitForSelector('text=待发起事项', {{ timeout: 10000 }});
          await page.waitForSelector('text=冷机月度巡检', {{ timeout: 10000 }});
          await page.waitForSelector('text=已开始未结束', {{ timeout: 10000 }});
          await page.waitForSelector('text=A楼UPS旁路切换变更测试', {{ timeout: 10000 }});
          await page.getByRole('button', {{ name: '刷新数据' }}).click();
          await page.waitForSelector('text=刷新检修', {{ timeout: 10000 }});
          await page.waitForSelector('text=刷新变更', {{ timeout: 10000 }});
          await page.keyboard.press('Escape');
          await page.waitForSelector('text=纯手填', {{ timeout: 10000 }});
          await page.waitForSelector('text=解析粘贴通告', {{ timeout: 10000 }});
          await page.waitForSelector('text=近三天可回退', {{ timeout: 10000 }});
          await page.locator('.recent-undo-panel').getByRole('button', {{ name: /展开/ }}).click();
          await page.waitForSelector('text=回退删除', {{ timeout: 10000 }});
          await page.getByRole('button', {{ name: '纯手填' }}).click();
          await page.waitForSelector('text=选择纯手填通告类型', {{ timeout: 10000 }});
          const manualAdjustButton = page.locator('.manual-type-grid button').filter({{ hasText: '调整' }});
          if (await manualAdjustButton.count() !== 1) throw new Error('manual adjust type button missing or ambiguous');
          await manualAdjustButton.click();
          await page.waitForSelector('text=待发起通告', {{ timeout: 10000 }});
          await page.locator('.drafts-panel').getByRole('button', {{ name: '编辑' }}).click();
          const draftPanelText = await page.locator('.drafts-panel').innerText({{ timeout: 10000 }});
          if (!draftPanelText.includes('通告类型') || !draftPanelText.includes('调整')) {{
            throw new Error(`manual adjust draft not visible: ${{draftPanelText}}`);
          }}
          await page.getByRole('button', {{ name: '解析粘贴' }}).click();
          await page.waitForSelector('text=解析到待发起通告', {{ timeout: 10000 }});
          const pastePanel = page.locator('.paste-panel');
          const pasteTextarea = pastePanel.locator('textarea[placeholder*="粘贴完整"]');
          await pasteTextarea.waitFor({{ timeout: 10000 }});
          await pasteTextarea.fill(`【设备调整】状态：开始
【名称】EA118机房A楼空调调整通告
【时间】2026-06-12 09:30~2026-06-12 18:30
【位置】A-101空调间
【内容】测试测试测试
【原因】测试测试测试
【影响】测试测试测试
【进度】测试测试测试`);
          await pastePanel.getByRole('button', {{ name: '解析到待发起通告' }}).click();
          await page.waitForSelector('text=EA118机房A楼空调调整通告', {{ timeout: 10000 }});
          const parsedDraftPanelText = await page.locator('.drafts-panel').innerText({{ timeout: 10000 }});
          if (!parsedDraftPanelText.includes('EA118机房A楼空调调整通告') || !parsedDraftPanelText.includes('调整')) {{
            throw new Error(`parsed adjust draft not visible: ${{parsedDraftPanelText}}`);
          }}
          const maintenanceSegment = page.locator('.segmented button').filter({{ hasText: '维保' }});
          if (await maintenanceSegment.count() !== 1) throw new Error('maintenance segment missing or ambiguous');
          await maintenanceSegment.click();
          await page.waitForSelector('text=冷机月度巡检', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, 'A楼 · 通告工作台', 'workbench');
          await assertLayout(page, 'workbench');
          await assertVnetSkin(page, 'workbench');
          await page.getByRole('button', {{ name: '功能选择' }}).click();
          await page.waitForSelector('text=业务工作台', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, '功能选择 · 请选择功能', 'home-after-return-from-workbench');
          await assertVnetSkin(page, 'home-after-return-from-workbench');
          await page.getByRole('button', {{ name: '进入维护管理', exact: true }}).click();
          await page.waitForSelector('text=选择楼栋进入维护管理', {{ timeout: 10000 }});
          const returnScopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await returnScopeCard.getByRole('button', {{ name: '进入维护管理' }}).click();
          await page.waitForSelector('text=待发起事项', {{ timeout: 10000 }});
          await page.waitForSelector('text=A楼UPS旁路切换变更测试', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, 'A楼 · 通告工作台', 'workbench-after-return');
          const secondPage = await context.newPage();
          attachDiagnostics(secondPage, 'auth-second-tab');
          await secondPage.goto(new URL('/?scope=A', cfg.url).toString(), {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          await secondPage.waitForSelector('text=待发起事项', {{ timeout: 10000 }});
          const allSegment = secondPage.locator('.segmented button').filter({{ hasText: '全部' }});
          if (await allSegment.count() !== 1) throw new Error('all work type segment missing or ambiguous');
          const allSegmentClass = await allSegment.first().getAttribute('class');
          if (!String(allSegmentClass || '').includes('active')) {{
            throw new Error(`all work type segment is not active by default: ${{allSegmentClass}}`);
          }}
          await secondPage.waitForSelector('text=冷机月度巡检', {{ timeout: 10000 }});
          await secondPage.waitForSelector('text=A楼UPS旁路切换变更测试', {{ timeout: 10000 }});
          await assertLayout(secondPage, 'second-tab-workbench');
          await page.waitForTimeout(1000);
          const streamUrls = [...await eventSourceUrls(page), ...await eventSourceUrls(secondPage)];
          const jobStreamCount = streamUrls.filter(url => url.includes('/api/jobs/stream')).length;
          const activeItemsStreamCount = streamUrls.filter(url => url.includes('/api/qt-active-items/stream')).length;
          if (jobStreamCount > 1 || activeItemsStreamCount > 1) {{
            throw new Error(`cross-tab stream coordination failed: jobs=${{jobStreamCount}} active=${{activeItemsStreamCount}} urls=${{streamUrls.join(',')}}`);
          }}
          const secondTabText = await secondPage.locator('body').innerText({{ timeout: 10000 }});
          if (secondTabText.includes('实时同步正在重连')) {{
            throw new Error('second tab should not show realtime reconnect warning while shared stream is active');
          }}
          await secondPage.close();
          await page.locator('article.notice-row').filter({{ hasText: '冷机月度巡检' }}).first().click();
          await page.waitForSelector('text=待发起通告', {{ timeout: 10000 }});
          await page.getByRole('button', {{ name: /发送.*开始/ }}).waitFor({{ timeout: 10000 }});
          await page.locator('article.ongoing-card').filter({{ hasText: 'A楼UPS旁路切换变更测试' }}).first().click();
          await page.waitForSelector('text=更新', {{ timeout: 10000 }});
          await page.waitForSelector('text=结束', {{ timeout: 10000 }});
          await page.getByRole('button', {{ name: '管理/诊断' }}).click();
          await page.waitForSelector('text=管理员工具', {{ timeout: 10000 }});
          await page.waitForSelector('text=查看详细诊断数据', {{ timeout: 10000 }});
          const rawDiagnosticOpen = await page.evaluate(() => {{
            const node = document.querySelector('.raw-diagnostic');
            return Boolean(node && node.hasAttribute('open'));
          }});
          if (rawDiagnosticOpen) throw new Error('admin raw diagnostic should be collapsed by default');
          await page.getByRole('button', {{ name: '权限' }}).click();
          await page.waitForSelector('text=保存权限', {{ timeout: 10000 }});
          await page.waitForSelector('text=添加用户', {{ timeout: 10000 }});
          let permissionSkin = await page.evaluate(() => {{
            const rows = Array.from(document.querySelectorAll('.permission-row'));
            const scopePills = Array.from(document.querySelectorAll('.permission-row .scope-checks label'));
            const activePills = scopePills.filter((node) => {{
              const input = node.querySelector('input[type="checkbox"]');
              return Boolean(input && input.checked);
            }});
            const firstRowStyle = rows[0] ? getComputedStyle(rows[0]) : null;
            return {{
              rowCount: rows.length,
              scopePillCount: scopePills.length,
              activePillCount: activePills.length,
              rowRadius: firstRowStyle?.borderRadius || '',
              rowBackground: firstRowStyle?.backgroundImage || firstRowStyle?.backgroundColor || '',
            }};
          }});
          if (permissionSkin.rowCount < 1) {{
            await page.getByRole('button', {{ name: '添加用户' }}).click();
            await page.waitForSelector('.permission-row', {{ timeout: 10000 }});
            permissionSkin = await page.evaluate(() => {{
              const rows = Array.from(document.querySelectorAll('.permission-row'));
              const scopePills = Array.from(document.querySelectorAll('.permission-row .scope-checks label'));
              const activePills = scopePills.filter((node) => {{
                const input = node.querySelector('input[type="checkbox"]');
                return Boolean(input && input.checked);
              }});
              const firstRowStyle = rows[0] ? getComputedStyle(rows[0]) : null;
              return {{
                rowCount: rows.length,
                scopePillCount: scopePills.length,
                activePillCount: activePills.length,
                rowRadius: firstRowStyle?.borderRadius || '',
                rowBackground: firstRowStyle?.backgroundImage || firstRowStyle?.backgroundColor || '',
              }};
            }});
          }}
          if (permissionSkin.rowCount < 1 || permissionSkin.scopePillCount < 1) {{
            throw new Error(`admin permission rows missing: ${{JSON.stringify(permissionSkin)}}`);
          }}
          const permissionRowRadius = Number.parseFloat(permissionSkin.rowRadius || '0');
          if (!Number.isFinite(permissionRowRadius) || permissionRowRadius < 12 || permissionRowRadius > 28 || !permissionSkin.rowBackground.includes('gradient')) {{
            throw new Error(`admin permission VNET skin missing: ${{JSON.stringify(permissionSkin)}}`);
          }}
          await page.getByRole('button', {{ name: '状态' }}).click();
          await page.waitForSelector('text=查看详细诊断数据', {{ timeout: 10000 }});
          await assertLayout(page, 'admin');
          await assertVnetSkin(page, 'admin');
          await page.goto(new URL('/admin/history-memory', cfg.url).toString(), {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          await page.waitForSelector('text=历史通告记忆导入', {{ timeout: 10000 }});
          await page.waitForSelector('text=扫描历史通告', {{ timeout: 10000 }});
          await page.waitForSelector('text=当前月事项', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, '管理工具 · 历史通告记忆导入', 'history-memory');
          await assertLayout(page, 'history-memory');
          await assertVnetSkin(page, 'history-memory');
          if (errors.length || failedResponses.length) {{
            throw new Error(`browser runtime errors: ${{errors.join(' | ')}} failedResponses=${{failedResponses.join(' | ')}}`);
          }}
          const pageTitle = await page.title().catch(() => '');
          await browser.close();
          console.log(JSON.stringify({{
            ok: true,
            title: pageTitle,
            markers: ['飞书扫码登录', ...required, 'A楼工作台', '待发起事项', '已开始未结束', '多标签SSE降噪', 'VNET蓝白皮肤', '管理员工具', '历史通告记忆导入'],
          }}));
        }})().catch(async err => {{
          console.error(String(err && err.stack || err));
          process.exit(1);
        }});
        """
    ).strip()


def run_smoke(*, port: int = 18976, keep_server_seconds: float = 0.0) -> dict:
    node = shutil.which("node")
    npx = shutil.which("npx")
    frontend_dir = BIN_DIR / "lan_bitable_template_portal" / "frontend"
    frontend_node_modules = frontend_dir / "node_modules"
    local_playwright = frontend_node_modules / "playwright"
    if not node and not npx:
        raise RuntimeError("node/npx 不可用，无法运行 Playwright 浏览器 smoke。")

    previous_mock = os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL")
    os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = "1"

    original_service = PortalRuntime.service
    original_auth = PortalRuntime.auth_manager
    original_store = PortalRuntime.state_store
    original_callbacks = (
        PortalRuntime.notice_callback,
        PortalRuntime.ongoing_callback,
        PortalRuntime.ongoing_delete_callback,
        PortalRuntime.maintenance_action_callback,
    )
    temp_dir = tempfile.TemporaryDirectory()
    server = None
    server_thread: threading.Thread | None = None
    try:
        import uvicorn

        state_store = LanPortalStateStore(Path(temp_dir.name) / "state.sqlite3")
        auth_manager = PortalAuthManager()
        auth_manager._state_store = state_store
        auth_manager.upsert_permission_user(
            open_id="ou_frontend_smoke",
            name="frontend-smoke-admin",
            role="admin",
            scopes=["ALL"],
            enabled=True,
            updated_by="frontend-runtime-smoke",
        )
        session_id = "frontend-runtime-smoke-session"
        no_scope_session_id = f"{session_id}-no-scope"
        with auth_manager._lock:
            auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {
                    "name": "frontend-smoke-admin",
                    "open_id": "ou_frontend_smoke",
                },
                "role": "admin",
                "allowed_scopes": ["ALL"],
                "scope_options": SCOPE_OPTIONS,
                "expires_at": time.time() + 3600,
            }
            auth_manager._sessions[no_scope_session_id] = {
                "session_id": no_scope_session_id,
                "user": {
                    "name": "frontend-smoke-no-scope",
                    "open_id": "ou_frontend_smoke_no_scope",
                },
                "role": "building",
                "allowed_scopes": [],
                "scope_options": [],
                "expires_at": time.time() + 3600,
            }

        PortalRuntime.service = _SmokePortalService()
        PortalRuntime.state_store = state_store
        PortalRuntime.auth_manager = auth_manager
        PortalRuntime.notice_callback = None
        PortalRuntime.ongoing_callback = None
        PortalRuntime.ongoing_delete_callback = None
        PortalRuntime.maintenance_action_callback = None

        controller = FastAPIPortalController(host="127.0.0.1", port=port)
        app = controller._build_app()
        bound_port = find_available_port("127.0.0.1", int(port or 18976))
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=bound_port,
            log_level="warning",
            access_log=False,
            log_config=None,
        )
        server = uvicorn.Server(config)

        def _serve() -> None:
            server.run()

        server_thread = threading.Thread(
            target=_serve,
            name="ClipFlowFrontendRuntimeSmoke",
            daemon=True,
        )
        server_thread.start()
        if not _wait_until_listening("127.0.0.1", bound_port, timeout_s=5.0):
            raise RuntimeError("smoke FastAPI 服务启动超时")

        url = f"http://127.0.0.1:{bound_port}/"
        with tempfile.TemporaryDirectory() as script_dir:
            script_path = Path(script_dir) / "frontend_smoke.js"
            script_path.write_text(_build_playwright_script(url, session_id), encoding="utf-8")
            env = dict(os.environ)
            env["npm_config_cache"] = str(Path(script_dir) / "npm-cache")
            env["npm_config_prefix"] = str(Path(script_dir) / "npm-prefix")
            if local_playwright.is_dir() and node:
                command = [node, str(script_path)]
                env["NODE_PATH"] = str(frontend_node_modules)
            elif npx:
                command = [
                    npx,
                    "--yes",
                    "--package",
                    "playwright",
                    "node",
                    str(script_path),
                ]
            else:
                raise RuntimeError(
                    "未找到本地 playwright 依赖，且 npx 不可用；请先在 frontend 执行 npm install。"
                )
            completed = subprocess.run(
                command,
                cwd=str(BIN_DIR.parent),
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                timeout=60,
            )
        if keep_server_seconds > 0:
            time.sleep(float(keep_server_seconds))
        if completed.returncode != 0:
            raise RuntimeError(
                (completed.stderr or completed.stdout or "Playwright smoke failed").strip()
            )
        output = (completed.stdout or "").strip().splitlines()[-1]
        data = json.loads(output)
        data["url"] = url
        return data
    finally:
        if server is not None:
            server.should_exit = True
        if server_thread and server_thread.is_alive():
            server_thread.join(timeout=3)
        PortalRuntime.service = original_service
        PortalRuntime.auth_manager = original_auth
        PortalRuntime.state_store = original_store
        (
            PortalRuntime.notice_callback,
            PortalRuntime.ongoing_callback,
            PortalRuntime.ongoing_delete_callback,
            PortalRuntime.maintenance_action_callback,
        ) = original_callbacks
        temp_dir.cleanup()
        if previous_mock is None:
            os.environ.pop("CLIPFLOW_BACKEND_MOCK_EXTERNAL", None)
        else:
            os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = previous_mock


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a real-browser smoke test for the Vue portal dist.")
    parser.add_argument("--port", type=int, default=18976)
    parser.add_argument("--keep-server-seconds", type=float, default=0.0)
    args = parser.parse_args()
    try:
        result = run_smoke(port=args.port, keep_server_seconds=args.keep_server_seconds)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
