/**
 * bloub-bot —— 零依赖的 x.ai 风格 bot 头像动画（单文件打包入口）。
 *
 * 用法：
 *   const bot = new BloubBot(el, { size: 240, color: 'encre', shape: 'cercle' })
 *   bot.play() / bot.pause() / bot.setState('orbit') / bot.setColor('bleu') ...
 *   bot.destroy()
 *
 * 引擎来自 https://github.com/jeremy-prt/bloub （MIT），本文件是其 BloubBot.vue
 * 渲染逻辑的 vanilla 重写：眼睛是 mask 挖出来的洞，弧线(orbits)分前后两层。
 */
import { BotEngine, type BotFrame } from './engine/engine'
import { NOTIF_BLUE } from './engine/decor'
import { EXPRESSION_BY_ID } from './engine/expressions'
import { DEMI_VIEWBOX, RAYON } from './engine/repere'
import { COLOR_BY_ID, DEFAULT_COLOR, DEFAULT_SHAPE, SHAPE_BY_ID, mixHex } from './engine/skins'
import { STATE_BY_ID, type StateId } from './engine/states'
import { SEQUENCE } from './engine/states'
import { lookTarget } from './gaze'
import { springStep } from './engine/math'

export type { StateId } from './engine/states'

export interface BloubOptions {
  /** 像素尺寸（宽=高），默认 320 */
  size?: number
  /** 形状 id：cercle | galet | squircle | capsule | triangle | hexagone | nuage | goutte */
  shape?: string
  /** 颜色 id：encre | creme | brun | rouge | orange | ambre | vert | turquoise | bleu | violet | rose | gris */
  color?: string
  /** 休息表情 id：neutre | attentif | surpris | excite | heureux | hilare | colere | triste | effraye | mefiant | confus | curieux | fier | timide | blase | somnolent */
  expression?: string
  /** 页面背景色（用于眼睛"洞"里透出的颜色、粒子雾化），默认 #f9f9f9 */
  paper?: string
  /** 播放状态序列（StateId[]），默认 14 个状态的完整循环 */
  cycle?: StateId[]
  /** 自动开始播放，默认 true */
  autoplay?: boolean
  /** 鼠标跟随（仅在 idle 等休息状态生效），默认 false */
  follow?: boolean
  /** 点击 bot 时随机做反应（wink/notify 等），默认 true */
  reactOnClick?: boolean
  /** 按住可以拖动 bot，松手弹性回弹；false 则原地不动，默认 true */
  draggable?: boolean
  /** 拖到新位置后回到原位（true=回弹，false=留在原地），默认 true */
  snapBack?: boolean
  onDragEnd?: (position: { x: number; y: number }, moved: boolean) => void
  dragTarget?: HTMLElement
}

export class BloubBot {
  private svg: SVGSVGElement
  private engine: BotEngine
  private opts: Required<Omit<BloubOptions, 'cycle' | 'onDragEnd' | 'dragTarget'>> & { cycle: StateId[] }
  private onDragEnd: BloubOptions['onDragEnd']
  private dragTarget: HTMLElement | undefined
  private frame: BotFrame
  private raf = 0
  private active = true
  private reducedMotion = false
  private renderedAt = -1
  private uid: string
  private clock = 0
  private last = -1
  private block = 0
  private blockStart = 0
  private playing: boolean
  private randomIdle = false
  private lastIdleAction: StateId | null = null
  private idleDuration = 5.5
  private disposed = false

  private ink: string
  private shapeRadii: number[] | null
  private expression: NonNullable<ReturnType<typeof EXPRESSION_BY_ID.get>> | null

  private pointer: { x: number; y: number } | null = null
  private pointerAt = -10
  private hovered = false
  private motionUntil = 0
  private greeting = false
  private scene: SVGGElement
  private motion = {
    lift: [0, 0], scale: [1, 0], roll: [0, 0], stretch: [0, 0],
  }
  private dragVelocity = { x: 0, y: 0 }
  private dragAt = 0

  // 拖动（pointer capture）与点击反应
  private dragging = false
  private dragPointerId: number | null = null
  private dragOffX = 0
  private dragOffY = 0
  private dragShiftX = 0
  private dragShiftY = 0
  private shiftDirty = false
  private homeX = 0
  private homeY = 0
  private settling = false
  private settleTargetX = 0
  private settleTargetY = 0
  private velocityX = 0
  private velocityY = 0
  private dragStartX = 0
  private dragStartY = 0
  private clickBlocked = false
  private reactingUntil = -1
  private reactionId: StateId | null = null

  private defs: SVGGElement
  private gradDefs: SVGGElement
  private layerArcBack: SVGGElement
  private layerDotsBehind: SVGGElement
  private layerBody: SVGGElement
  private layerMask: SVGMaskElement
  private pathBodyMask: SVGPathElement
  private groupEyesMask: SVGGElement
  private circleNotchMask: SVGCircleElement
  private pathBodyPaper: SVGPathElement
  private groupInk: SVGGElement
  private rectInk: SVGRectElement
  private layerDotsFront: SVGGElement
  private circleNotif: SVGCircleElement
  private layerArcFront: SVGGElement

  private readonly fnTick = (ms: number) => this.tick(ms)
  private readonly fnMove = (e: PointerEvent) => {
    if (e.pointerType === 'touch') return
    this.pointer = { x: e.clientX, y: e.clientY }
    this.pointerAt = this.clock
  }
  private readonly fnLeave = () => {
    this.pointer = null
  }
  private readonly fnHover = () => { this.hovered = true; this.motionUntil = this.clock + 0.7 }
  private readonly fnHoverEnd = () => { this.hovered = false; this.motionUntil = this.clock + 0.7 }
  private readonly fnDown = (e: PointerEvent) => {
    if (!this.opts.draggable || e.button !== 0) return
    this.dragging = true
    this.dragPointerId = e.pointerId
    const r = this.svg.getBoundingClientRect()
    this.homeX = r.left - this.dragShiftX
    this.homeY = r.top - this.dragShiftY
    this.dragStartX = e.clientX
    this.dragStartY = e.clientY
    this.dragAt = performance.now()
    this.dragVelocity = { x: 0, y: 0 }
    this.velocityX = this.velocityY = 0
    if (this.randomIdle) {
      this.block = 0
      this.blockStart = this.clock
      this.reactingUntil = -1
      this.reactionId = null
      this.engine.setState('idle', this.clock)
    }
    this.engine.setExpression(EXPRESSION_BY_ID.get('surpris') ?? this.expression, this.clock)
    this.clickBlocked = false
    this.settling = false
    this.dragOffX = e.clientX - r.left
    this.dragOffY = e.clientY - r.top
    try {
      this.svg.setPointerCapture(e.pointerId)
    } catch {
      /* 老浏览器忽略 */
    }
  }
  private readonly fnDrag = (e: PointerEvent) => {
    if (!this.dragging || e.pointerId !== this.dragPointerId) return
    if (Math.hypot(e.clientX - this.dragStartX, e.clientY - this.dragStartY) > 6) this.clickBlocked = true
    // 把拖动偏移折算成屏幕像素位移：以 bot 当前尺寸为基准
    const now = performance.now(), dt = Math.max(0.008, (now - this.dragAt) / 1000)
    const x = e.clientX - this.dragOffX - this.homeX, y = e.clientY - this.dragOffY - this.homeY
    this.dragVelocity.x = Math.max(-1800, Math.min(1800, (x - this.dragShiftX) / dt))
    this.dragVelocity.y = Math.max(-1800, Math.min(1800, (y - this.dragShiftY) / dt))
    this.dragAt = now
    this.dragShiftX = x
    this.dragShiftY = y
    this.clampShift()
    this.shiftDirty = true
    this.pointer = { x: e.clientX, y: e.clientY }
  }
  private readonly fnUp = () => {
    if (!this.dragging) return
    if (this.shiftDirty) this.applyShift()
    this.dragging = false
    this.engine.setExpression(this.expression, this.clock)
    this.motionUntil = this.clock + 0.9
    if (this.clickBlocked && !this.reducedMotion) {
      this.motion.lift[1] = -90
      this.motion.scale[1] = -0.6
      this.blockStart = this.clock
    }
    if (this.dragPointerId !== null) {
      try { this.svg.releasePointerCapture(this.dragPointerId) } catch { /* already released */ }
      this.dragPointerId = null
    }
    if (this.opts.snapBack && Math.hypot(this.dragShiftX, this.dragShiftY) > 2) {
      if (this.active) this.settling = true
      else this.setPosition(null)
      this.settleTargetX = 0
      this.settleTargetY = 0
      this.velocityX = this.velocityY = 0
    }
    this.onDragEnd?.({ x: this.homeX + this.dragShiftX, y: this.homeY + this.dragShiftY }, this.clickBlocked)
  }
  private readonly fnClickReact = (event: MouseEvent) => {
    // 拖动手势结束时不触发点按反应
    if (this.clickBlocked) {
      this.clickBlocked = false
      event.preventDefault()
      event.stopPropagation()
      return
    }
    if (!this.opts.reactOnClick || !this.active || !STATE_BY_ID.get(this.engine.state)?.baseFace) return
    // 只有"休息脸"状态适合做表情反应；点按优先 wink / notify
    const reactions: StateId[] = ['wink', 'notify']
    const id = reactions[Math.floor(Math.random() * reactions.length)] as StateId
    this.engine.setState(id, this.clock)
    this.reactingUntil = this.clock + 1.6
    this.reactionId = id
  }

  constructor(target: HTMLElement, options: BloubOptions = {}) {
    this.onDragEnd = options.onDragEnd
    this.dragTarget = options.dragTarget
    this.opts = {
      size: options.size ?? 320,
      shape: options.shape ?? DEFAULT_SHAPE,
      color: options.color ?? DEFAULT_COLOR,
      expression: options.expression ?? 'neutre',
      paper: options.paper ?? '#f9f9f9',
      cycle: options.cycle ?? (SEQUENCE as StateId[]),
      autoplay: options.autoplay ?? true,
      follow: options.follow ?? false,
      reactOnClick: options.reactOnClick ?? true,
      draggable: options.draggable ?? true,
      snapBack: options.snapBack ?? true
    }
    this.playing = this.opts.autoplay
    this.ink = COLOR_BY_ID.get(this.opts.color)?.hex ?? '#0a0a0c'
    this.shapeRadii = SHAPE_BY_ID.get(this.opts.shape)?.radii ?? null
    this.expression = EXPRESSION_BY_ID.get(this.opts.expression) ?? null
    this.uid = 'bb' + Math.random().toString(36).slice(2, 8)

    this.engine = new BotEngine(RAYON, this.opts.cycle[0] ?? 'idle', this.shapeRadii, this.expression)
    this.engine.setLook(lookTarget({ nx: 0, ny: 0, pointer: false }), -1)
    this.frame = this.engine.sample(0)

    this.svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg')
    this.svg.setAttribute('viewBox', `${-DEMI_VIEWBOX} ${-DEMI_VIEWBOX} ${DEMI_VIEWBOX * 2} ${DEMI_VIEWBOX * 2}`)
    this.svg.setAttribute('role', 'img')
    this.svg.setAttribute('aria-label', 'bloub bot')
    this.svg.classList.add('bloub-bot')
    this.svg.style.display = 'block'
    this.svg.style.width = `${this.opts.size}px`
    this.svg.style.height = `${this.opts.size}px`

    // ---- defs：mask（眼睛是挖出来的洞）+ 渐变 ----
    this.defs = this.el('defs') as SVGGElement
    this.svg.appendChild(this.defs)

    const mask = this.el('mask') as SVGMaskElement
    mask.setAttribute('id', `${this.uid}-m`)  // groupInk 用 url(#${this.uid}-m) 引用
    mask.setAttribute('maskUnits', 'userSpaceOnUse')
    mask.setAttribute('x', String(-DEMI_VIEWBOX))
    mask.setAttribute('y', String(-DEMI_VIEWBOX))
    mask.setAttribute('width', String(DEMI_VIEWBOX * 2))
    mask.setAttribute('height', String(DEMI_VIEWBOX * 2))
    this.pathBodyMask = this.el('path', { fill: '#fff' }) as SVGPathElement
    this.groupEyesMask = this.el('g') as SVGGElement
    this.circleNotchMask = this.el('circle', { fill: '#000' }) as SVGCircleElement
    mask.appendChild(this.pathBodyMask)
    mask.appendChild(this.groupEyesMask)
    mask.appendChild(this.circleNotchMask)
    this.layerMask = mask
    this.defs.appendChild(mask)

    this.gradDefs = this.el('g') as SVGGElement
    this.defs.appendChild(this.gradDefs)
    this.scene = this.el('g', { 'data-bot-scene': '' }) as SVGGElement
    this.svg.appendChild(this.scene)

    // ---- 图层顺序与 BloubBot.vue 完全一致 ----
    this.layerArcBack = this.el('g', { fill: 'none', 'stroke-linecap': 'round' }) as SVGGElement
    this.scene.appendChild(this.layerArcBack)

    this.layerDotsBehind = this.el('g') as SVGGElement
    this.scene.appendChild(this.layerDotsBehind)

    // 身体：纸色打底（填满眼睛洞能看到的位置）→ mask 挖眼 → 墨色身体
    this.layerBody = this.el('g') as SVGGElement
    this.scene.appendChild(this.layerBody)
    this.pathBodyPaper = this.el('path', { fill: this.opts.paper }) as SVGPathElement
    this.layerBody.appendChild(this.pathBodyPaper)
    this.groupInk = this.el('g') as SVGGElement
    this.groupInk.setAttribute('mask', `url(#${this.uid}-m)`)
    this.layerBody.appendChild(this.groupInk)
    this.rectInk = this.el('rect', { fill: this.ink }) as SVGRectElement
    this.rectInk.setAttribute('x', String(-DEMI_VIEWBOX))
    this.rectInk.setAttribute('y', String(-DEMI_VIEWBOX))
    this.rectInk.setAttribute('width', String(DEMI_VIEWBOX * 2))
    this.rectInk.setAttribute('height', String(DEMI_VIEWBOX * 2))
    this.groupInk.appendChild(this.rectInk)

    this.layerDotsFront = this.el('g') as SVGGElement
    this.scene.appendChild(this.layerDotsFront)

    this.circleNotif = this.el('circle', { fill: NOTIF_BLUE }) as SVGCircleElement
    this.scene.appendChild(this.circleNotif)

    this.layerArcFront = this.el('g', { fill: 'none', 'stroke-linecap': 'round' }) as SVGGElement
    this.scene.appendChild(this.layerArcFront)

    target.appendChild(this.svg)

    // 记录初始位置（CSS transform 的基点）
    const rect0 = this.svg.getBoundingClientRect()
    this.homeX = rect0.left
    this.homeY = rect0.top

    if (this.opts.follow) {
      window.addEventListener('pointermove', this.fnMove, { capture: true, passive: true })
      document.addEventListener('pointerleave', this.fnLeave)
    }
    // 点按反应 + 拖动
    this.svg.addEventListener('pointerdown', this.fnDown)
    this.svg.addEventListener('pointermove', this.fnDrag)
    this.svg.addEventListener('pointerup', this.fnUp)
    this.svg.addEventListener('pointercancel', this.fnUp)
    this.svg.addEventListener('lostpointercapture', this.fnUp)
    window.addEventListener('blur', this.fnUp)
    this.svg.addEventListener('click', this.fnClickReact)
    this.svg.addEventListener('pointerenter', this.fnHover)
    this.svg.addEventListener('pointerleave', this.fnHoverEnd)
    this.render(this.frame)
    this.raf = requestAnimationFrame(this.fnTick)
  }

  private el(name: string, attrs: Record<string, string> = {}): Element {
    const node = document.createElementNS('http://www.w3.org/2000/svg', name)
    for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v)
    return node
  }

  private attr(node: Element, name: string, value: string) {
    if (node.getAttribute(name) !== value) node.setAttribute(name, value)
  }

  /* -------------------------------------------------------------  公共 API  */

  /** 开始播放状态序列 */
  play() {
    if (this.playing) return
    this.playing = true
    this.blockStart = this.clock
  }

  /** 暂停序列（bot 仍会呼吸、眨眼） */
  pause() {
    this.playing = false
  }

  /** Suspend rendering while the page is hidden. */
  setActive(active: boolean) {
    if (this.disposed) return
    if (!active) {
      this.active = false
      if (this.dragging) { this.clickBlocked = true; this.fnUp() }
      cancelAnimationFrame(this.raf)
      this.raf = 0
      this.last = -1
      if (this.settling) this.setPosition({ x: this.homeX + this.settleTargetX, y: this.homeY + this.settleTargetY })
      this.frame = this.engine.sample(this.clock + 0.6)
      this.render(this.frame)
    } else if (!this.active) {
      this.active = true
      this.last = -1
      this.renderedAt = -1
      this.raf = requestAnimationFrame(this.fnTick)
    }
  }

  /** Keep eye feedback and transitions, without decorative bouncing or tilting. */
  setReducedMotion(value: boolean) { this.reducedMotion = value }

  /** 跳到某个状态（跨状态有 morph 过渡）；frozen 设备下自动从该状态继续播放 */
  setState(id: StateId) {
    this.randomIdle = false
    this.opts.cycle = [id]
    this.block = 0
    this.blockStart = this.clock
    this.reactingUntil = -1
    this.reactionId = null
    this.engine.setState(id, this.clock)
  }

  /** 恢复默认完整循环（14 个状态）并从第一个状态重新开始播放 */
  setSysCycle() {
    this.setCycle(SEQUENCE as StateId[])
  }

  /** Random idle mode alternates a resting face with a non-repeating action. */
  setCycle(states: StateId[], randomIdle = false) {
    randomIdle = randomIdle && states[0] === 'idle' && states.length > 1
    if (this.playing && this.randomIdle === randomIdle && states.length === this.opts.cycle.length && states.every((state, i) => state === this.opts.cycle[i])) return
    this.randomIdle = randomIdle
    this.idleDuration = randomIdle ? 6 + Math.random() * 4 : 5.5
    this.opts.cycle = [...states]
    this.block = 0
    this.blockStart = this.clock
    this.playing = true
    this.reactingUntil = -1
    this.reactionId = null
    this.engine.setState(this.opts.cycle[0] ?? 'idle', this.clock)
  }

  /** 设置主题色（encre/bleu/vert...） */
  setColor(id: string) {
    this.opts.color = id
    this.ink = COLOR_BY_ID.get(id)?.hex ?? '#0a0a0c'
    if (!this.disposed) this.rectInk.setAttribute('fill', this.ink)
  }

  /** 设置身体形状（cercle/galet/squircle/capsule/triangle/hexagone/nuage/goutte），带动画 morph */
  setShape(id: string) {
    this.opts.shape = id
    this.shapeRadii = SHAPE_BY_ID.get(id)?.radii ?? null
    this.engine.setShape(this.shapeRadii, this.clock)
  }

  /** 设置休息表情（neutre/colere/heureux...） */
  setExpression(id: string) {
    this.opts.expression = id
    this.expression = EXPRESSION_BY_ID.get(id) ?? null
    this.engine.setExpression(this.dragging ? EXPRESSION_BY_ID.get('surpris') ?? this.expression : this.expression, this.clock)
    this.motionUntil = this.clock + 0.6
  }

  /** 鼠标跟随开关（仅休息脸状态有效） */
  followPointer(x: number, y: number) {
    if (!this.opts.follow || !Number.isFinite(x) || !Number.isFinite(y)) return
    this.pointer = { x, y }
    this.pointerAt = this.clock
  }

  setFollow(on: boolean) {
    if (this.opts.follow === on) return
    this.opts.follow = on
    window.removeEventListener('pointermove', this.fnMove, true)
    document.removeEventListener('pointerleave', this.fnLeave)
    if (on) {
      window.addEventListener('pointermove', this.fnMove, { capture: true, passive: true })
      document.addEventListener('pointerleave', this.fnLeave)
    } else {
      this.pointer = null
      this.engine.setLook(lookTarget({ nx: 0, ny: 0, pointer: false }), this.clock)
    }
  }

  /** 设置像素尺寸 */
  setSize(px: number) {
    this.opts.size = px
    if (!this.disposed) {
      this.svg.style.width = `${px}px`
      this.svg.style.height = `${px}px`
    }
  }

  /** Restore a retained screen position; null returns to the CSS anchor. */
  setPosition(position: { x: number; y: number } | null) {
    if (this.disposed || this.dragging) return
    const rect = this.svg.getBoundingClientRect()
    this.homeX = rect.left - this.dragShiftX
    this.homeY = rect.top - this.dragShiftY
    this.dragShiftX = position ? position.x - this.homeX : 0
    this.dragShiftY = position ? position.y - this.homeY : 0
    if (position) this.clampShift()
    this.settling = false
    this.velocityX = this.velocityY = 0
    this.applyShift()
  }

  /** Overlay avoidance shares the renderer's clock, without a second RAF loop. */
  moveTo(position: { x: number; y: number }) {
    if (this.disposed || this.dragging) return
    if (!this.active) { this.setPosition(position); return }
    const rect = this.svg.getBoundingClientRect()
    this.homeX = rect.left - this.dragShiftX
    this.homeY = rect.top - this.dragShiftY
    const x = position.x - this.homeX, y = position.y - this.homeY
    if (this.settling && Math.hypot(x - this.settleTargetX, y - this.settleTargetY) < 0.5) return
    if (Math.hypot(x - this.dragShiftX, y - this.dragShiftY) < 0.5) return
    this.settleTargetX = x; this.settleTargetY = y
    this.settling = true
  }

  greet() { if (this.active) this.greeting = true }

  playback() {
    return { ...this.engine.playback(this.clock), clock: this.clock, block: this.block, blockElapsed: this.clock - this.blockStart, reactionRemaining: Math.max(0, this.reactingUntil - this.clock), lastIdleAction: this.lastIdleAction, idleDuration: this.idleDuration }
  }

  restorePlayback(value: unknown): boolean {
    if (!value || typeof value !== 'object') return false
    const data = value as ReturnType<BloubBot['playback']>
    if (!STATE_BY_ID.has(data.state) || ![data.clock, data.elapsed, data.blockElapsed, data.block].every(n => Number.isFinite(n) && n >= 0 && n < 1e7)) return false
    if (!data.look || !['yaw', 'pitch', 'mix', 'spin', 'wander'].every(key => Number.isFinite(data.look[key as keyof typeof data.look]) && Math.abs(data.look[key as keyof typeof data.look]) <= 360)) return false
    if (!Number.isInteger(data.block) || data.block >= this.opts.cycle.length) return false
    if (!(data.reactionRemaining > 0) && data.state !== this.opts.cycle[data.block]) return false
    this.clock = data.clock; this.block = data.block; this.blockStart = this.clock - data.blockElapsed
    this.lastIdleAction = data.lastIdleAction && this.opts.cycle.includes(data.lastIdleAction) ? data.lastIdleAction : null
    if (this.randomIdle && Number.isFinite(data.idleDuration) && data.idleDuration >= 6 && data.idleDuration <= 10) this.idleDuration = data.idleDuration
    this.reactingUntil = Number.isFinite(data.reactionRemaining) && data.reactionRemaining > 0 ? this.clock + Math.min(2, data.reactionRemaining) : -1
    this.reactionId = this.reactingUntil > 0 ? data.state : null
    this.engine.reset(data.state, this.clock - data.elapsed)
    this.engine.setLook(data.look, this.clock - 1)
    this.frame = this.engine.sample(this.clock); this.render(this.frame)
    return true
  }

  private clampShift() {
    this.dragShiftX = Math.min(Math.max(this.homeX + this.dragShiftX, 12), Math.max(12, window.innerWidth - this.opts.size - 12)) - this.homeX
    this.dragShiftY = Math.min(Math.max(this.homeY + this.dragShiftY, 12), Math.max(12, window.innerHeight - this.opts.size - 12)) - this.homeY
  }

  /** 释放：停止动画并移除 DOM */
  destroy() {
    if (this.disposed) return
    this.disposed = true
    cancelAnimationFrame(this.raf)
    window.removeEventListener('pointermove', this.fnMove, true)
    document.removeEventListener('pointerleave', this.fnLeave)
    this.svg.removeEventListener('pointerdown', this.fnDown)
    this.svg.removeEventListener('pointermove', this.fnDrag)
    this.svg.removeEventListener('pointerup', this.fnUp)
    this.svg.removeEventListener('pointercancel', this.fnUp)
    this.svg.removeEventListener('lostpointercapture', this.fnUp)
    window.removeEventListener('blur', this.fnUp)
    this.svg.removeEventListener('click', this.fnClickReact)
    this.svg.removeEventListener('pointerenter', this.fnHover)
    this.svg.removeEventListener('pointerleave', this.fnHoverEnd)
    this.svg.remove()
  }

  /** 把当前 shift 写到 SVG 的 CSS transform 上（拖动/回弹共用） */
  private applyShift() {
    if (this.disposed) return
    this.shiftDirty = false
    ;(this.dragTarget ?? this.svg).style.transform = `translate3d(${this.dragShiftX}px, ${this.dragShiftY}px, 0)`
  }

  private settle(dt: number) {
    if (this.disposed || !this.settling) return
    ;[this.dragShiftX, this.velocityX] = springStep(this.dragShiftX, this.velocityX, this.settleTargetX, dt)
    ;[this.dragShiftY, this.velocityY] = springStep(this.dragShiftY, this.velocityY, this.settleTargetY, dt)
    if (Math.hypot(this.dragShiftX - this.settleTargetX, this.dragShiftY - this.settleTargetY) < 0.25 && Math.hypot(this.velocityX, this.velocityY) < 4) {
      this.dragShiftX = this.settleTargetX
      this.dragShiftY = this.settleTargetY
      this.settling = false
      this.velocityX = this.velocityY = 0
    }
    this.applyShift()
  }

  /** 暴露引擎顶层样式（进阶用法） */
  get engineInstance() {
    return this.engine
  }

  /** 公开读写：拖动后是否回弹原位（默认 true；false = 留在原地） */
  get snapBack(): boolean {
    return this.opts.snapBack
  }
  set snapBack(v: boolean) {
    this.opts.snapBack = v
  }

  /* -------------------------------------------------------------  内部渲染  */

  private tick(ms: number) {
    if (this.disposed || !this.active) return
    this.raf = requestAnimationFrame(this.fnTick)
    const moving = this.dragging || this.settling || this.greeting || this.clock < this.motionUntil || this.clock - this.pointerAt < 0.35
    // Smooth interaction at 60fps; quiet idle animation stays at 30fps.
    if (this.renderedAt >= 0 && ms - this.renderedAt < 1000 / (moving ? 60 : 30) - 0.5) return
    this.renderedAt = ms
    const dt = this.last >= 0 ? Math.min((ms - this.last) / 1000, 0.064) : 0
    this.last = ms
    this.clock += dt

    if (this.greeting) {
      this.greeting = false
      if (!this.reducedMotion) { this.motion.lift[1] = -180; this.motion.scale[1] = 1.5 }
      this.motionUntil = this.clock + 0.9
      if (STATE_BY_ID.get(this.engine.state)?.baseFace) {
        this.engine.setState('wink', this.clock); this.reactionId = 'wink'; this.reactingUntil = this.clock + 0.95
      }
    }

    const blocks = this.opts.cycle
    // Click reactions must also return to idle when sequence playback is off.
    if (blocks.length && this.reactingUntil >= 0 && this.clock >= this.reactingUntil) {
        this.reactingUntil = -1
        const cur = blocks[this.block]!
        if (this.reactionId !== cur) this.engine.setState(cur, this.clock)
        this.reactionId = null
    }
    const idleInterrupted = this.randomIdle && (this.dragging || this.settling || this.hovered)
    if (idleInterrupted) this.blockStart += dt
    if (this.playing && blocks.length && !this.dragging && !idleInterrupted) {
      const dur = this.blockDuration(this.block)
      const elapsed = this.clock - this.blockStart
      if (this.reactingUntil < 0 && elapsed >= dur) {
        if (this.randomIdle) {
          if (this.block === 0) {
            const choices = blocks.map((state, index) => ({ state, index }))
              .filter(({ state, index }) => index > 0 && state !== 'idle' && state !== this.lastIdleAction)
            this.block = choices.length ? choices[Math.floor(Math.random() * choices.length)]!.index : 1
            this.lastIdleAction = blocks[this.block]!
          } else {
            this.block = 0
            this.idleDuration = 6 + Math.random() * 4
          }
        } else this.block = (this.block + 1) % blocks.length
        this.blockStart = this.clock
        const next = blocks[this.block]!
        this.engine.setState(next, this.clock)
      }
    }

    if (this.settling) this.settle(dt)
    else if (this.shiftDirty) this.applyShift()

    this.aim()
    this.animateBody(dt)
    this.frame = this.engine.sample(this.clock)
    this.render(this.frame)
  }

  private blockDuration(i: number): number {
    const blocks = this.opts.cycle
    const id = blocks[i]
    return id === 'idle' ? this.idleDuration : STATE_BY_ID.get(id)?.duration ?? 2
  }

  private animateBody(dt: number) {
    const speed = this.dragging ? Math.min(1, Math.hypot(this.dragVelocity.x, this.dragVelocity.y) / 1200) : 0
    const targets = {
      lift: this.dragging ? -4 : this.hovered ? -1.8 : 0,
      scale: this.dragging ? 1.08 : this.hovered ? 1.035 : 1,
      roll: this.dragging ? Math.max(-8, Math.min(8, this.dragVelocity.x / 150)) : this.hovered ? -2 : 0,
      stretch: speed * 0.07,
    }
    if (this.reducedMotion) Object.assign(targets, { lift: 0, scale: 1, roll: 0, stretch: 0 })
    for (const key of Object.keys(targets) as (keyof typeof targets)[]) {
      const [value, velocity] = this.motion[key]
      this.motion[key] = springStep(value, velocity, targets[key], dt, 17)
    }
    const { lift, scale, roll, stretch } = this.motion
    this.attr(this.scene, 'transform', `translate(0 ${lift[0].toFixed(3)}) rotate(${roll[0].toFixed(3)}) scale(${(scale[0] + stretch[0]).toFixed(4)} ${(scale[0] - stretch[0]).toFixed(4)})`)
  }

  private aim() {
    // Position is refreshed on mount, resize and explicit movement.
    // Reading layout after SVG writes on every frame forces the page to reflow.
    const centerX = this.homeX + this.dragShiftX + this.opts.size / 2
    const centerY = this.homeY + this.dragShiftY + this.opts.size / 2
    // Use the reachable page area, not half a viewport that saturates at the edges.
    const rangeX = Math.max(240, centerX, window.innerWidth - centerX)
    const rangeY = Math.max(180, centerY, window.innerHeight - centerY)
    const pointer = this.opts.follow ? this.pointer : null
    const nx = pointer
      ? (pointer.x - centerX) / rangeX
      : 0
    const ny = pointer
      ? (pointer.y - centerY) / rangeY
      : 0
    this.engine.setLook(
      lookTarget({ nx, ny, pointer: pointer !== null }),
      this.clock,
      0.35
    )
  }

  private render(f: BotFrame) {
    const vb = DEMI_VIEWBOX

    // --- mask（身体白色 + 眼睛黑色洞 + 通知凹口）---
    this.attr(this.pathBodyMask, 'd', f.bodyPath)
    while (this.groupEyesMask.childElementCount > f.eyes.length) this.groupEyesMask.lastElementChild?.remove()
    for (const [index, eye] of f.eyes.entries()) {
      let p = this.groupEyesMask.children[index]
      if (!p) {
        p = this.el('path', { fill: '#000' })
        this.groupEyesMask.appendChild(p)
      }
      this.attr(p, 'd', eye.d)
      this.attr(p, 'transform', eye.matrix)
      this.attr(p, 'opacity', String(eye.alpha))
    }
    this.circleNotchMask.style.display = f.notch ? '' : 'none'
    if (f.notch) {
      this.attr(this.circleNotchMask, 'cx', String(f.notch.x))
      this.attr(this.circleNotchMask, 'cy', String(f.notch.y))
      this.attr(this.circleNotchMask, 'r', String(f.notch.r))
    }

    // --- 渐变（每个 arc 一个）---
    while (this.gradDefs.childElementCount > f.arcs.length) this.gradDefs.lastElementChild?.remove()
    const gradIds: string[] = []
    for (const [index, arc] of f.arcs.entries()) {
      let g = this.gradDefs.children[index]
      if (!g) { g = this.el('linearGradient'); this.gradDefs.appendChild(g) }
      this.attr(g, 'gradientUnits', 'userSpaceOnUse')
      this.attr(g, 'x1', String(arc.grad.x1))
      this.attr(g, 'y1', String(arc.grad.y1))
      this.attr(g, 'x2', String(arc.grad.x2))
      this.attr(g, 'y2', String(arc.grad.y2))
      const stops = arc.grad.stops
      while (g.childElementCount > stops.length) g.lastElementChild?.remove()
      stops.forEach((c, i) => {
        let s = g.children[i]
        if (!s) { s = this.el('stop'); g.appendChild(s) }
        this.attr(s, 'offset', String(i / Math.max(1, stops.length - 1)))
        this.attr(s, 'stop-color', c)
      })
      const id = `${this.uid}-g${gradIds.length}`
      this.attr(g, 'id', id)
      gradIds.push(id)
    }

    // --- 后层弧线（会被身体遮挡）---
    this.setArcs(this.layerArcBack, f.arcs, gradIds)

    // --- 后层粒子 ---
    this.layerDotsBehind.style.display = f.dotsBehind ? '' : 'none'
    if (f.dotsBehind) this.setDots(this.layerDotsBehind, f.dots)

    // --- 身体 ---
    const fillInk = this.ink
    const paper = this.opts.paper
    const dots = f.dots
    this.attr(this.pathBodyPaper, 'd', f.bodyPath)
    this.attr(this.rectInk, 'fill', fillInk)
    this.attr(this.layerBody, 'opacity', String(f.bodyAlpha))

    // --- 前层粒子 ---
    this.layerDotsFront.style.display = f.dotsBehind ? 'none' : ''
    if (!f.dotsBehind) this.setDots(this.layerDotsFront, dots)

    // --- 通知蓝点 ---
    this.circleNotif.style.display = f.notif ? '' : 'none'
    if (f.notif) {
      this.attr(this.circleNotif, 'cx', String(f.notif.x))
      this.attr(this.circleNotif, 'cy', String(f.notif.y))
      this.attr(this.circleNotif, 'r', String(f.notif.r))
    }

    // --- 前层弧线 ---
    this.setArcs(this.layerArcFront, f.arcs, gradIds)

    // 越界保护不是必要的（viewBox 恒为 158），但保留变量以免误用
    void vb
  }

  private setArcs(layer: SVGGElement, arcs: BotFrame['arcs'], gradIds: string[]) {
    while (layer.childElementCount > arcs.length) layer.lastElementChild?.remove()
    arcs.forEach((arc, i) => {
      let p = layer.children[i]
      if (!p) { p = this.el('path'); layer.appendChild(p) }
      this.attr(p, 'd', layer === this.layerArcBack ? arc.back : arc.front)
      this.attr(p, 'stroke', `url(#${gradIds[i]})`)
      this.attr(p, 'stroke-width', String(arc.width))
      this.attr(p, 'opacity', String(arc.opacity))
    })
  }

  private setDots(layer: SVGGElement, dots: BotFrame['dots']) {
    while (layer.childElementCount > dots.length) layer.lastElementChild?.remove()
    for (const [index, dot] of dots.entries()) {
      const fill =
        dot.color ??
        (dot.depth === undefined ? this.ink : mixHex(this.opts.paper, this.ink, dot.depth))
      const tag = dot.d ? 'path' : 'circle'
      let node = layer.children[index]
      if (!node) { node = this.el(tag); layer.appendChild(node) }
      else if (node.tagName !== tag) { const replacement = this.el(tag); node.replaceWith(replacement); node = replacement }
      this.attr(node, 'fill', fill); this.attr(node, 'opacity', String(dot.opacity))
      if (dot.d) {
        this.attr(node, 'd', dot.d)
        this.attr(node, 'transform', `translate(${dot.x} ${dot.y}) rotate(${dot.rot ?? 0}) scale(${RAYON})`)
      } else {
        this.attr(node, 'cx', String(dot.x)); this.attr(node, 'cy', String(dot.y)); this.attr(node, 'r', String(dot.r))
      }
    }
  }
}

/** 便捷：把 bot 挂到指定容器并返回实例 */
export function createBloubBot(target: HTMLElement, options?: BloubOptions): BloubBot {
  return new BloubBot(target, options)
}
