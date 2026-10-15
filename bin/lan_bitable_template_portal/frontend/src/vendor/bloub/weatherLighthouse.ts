import artwork from './weather-lighthouse.svg?raw';
import type { BotFrame } from './engine/engine';
import { DEMI_VIEWBOX } from './engine/repere';
import type { WeatherScene } from '../../botWeather';
import { miniLighting, moonPath, waterFrames } from './weatherLighthouseMini';
import './weatherLighthouse.css';

export class WeatherLighthouse {
  readonly root: SVGGElement;
  private eyes: SVGPathElement[];
  private sun: SVGGElement;
  private moon: SVGGElement;
  private stars: SVGGElement;
  private companion: SVGGElement;
  private hits: SVGElement[];
  private waterAnimations: Animation[] = [];
  private moonLight?: SVGPathElement;
  private moonClip?: SVGPathElement;
  private towerGradients: SVGLinearGradientElement[] = [];
  private random(min: number, max: number) { return min + Math.random() * (max - min); }
  private vary(node: SVGElement) {
    if (node.classList.contains('wl-star')) {
      node.setAttribute('cx', this.random(225, 535).toFixed(1));
      node.setAttribute('cy', this.random(38, 240).toFixed(1));
      node.setAttribute('r', this.random(1.4, 3).toFixed(1));
      node.style.setProperty('--wl-brightness', this.random(.35, .95).toFixed(2));
    } else if (node.classList.contains('wl-cloud-flight')) {
      node.style.setProperty('--wl-cloud-y', this.random(95, 240).toFixed(1) + 'px');
      node.style.setProperty('--wl-cloud-scale', this.random(.65, 1.1).toFixed(2));
      node.style.setProperty('--wl-cloud-opacity', (this.mini ? this.random(.72, .95) : this.random(.45, .8)).toFixed(2));
      if (this.mini) node.style.animationDirection = Math.random() < .5 ? 'normal' : 'reverse';
    } else if (node.classList.contains('wl-heat-wisp')) {
      node.style.setProperty('--wl-heat-x', this.random(220, 535).toFixed(1) + 'px');
      node.style.setProperty('--wl-heat-y', this.random(270, 335).toFixed(1) + 'px');
      node.style.setProperty('--wl-heat-width', this.random(.65, 1.4).toFixed(2));
      node.style.setProperty('--wl-heat-height', this.random(.7, 1.2).toFixed(2));
    } else if (node.closest('.wl-waves')) {
      if (this.mini) return;
      node.style.setProperty('--wl-swell', this.random(.8, 4).toFixed(2) + 'px');
      node.style.setProperty('--wl-wave-height', this.random(.95, 1.3).toFixed(2));
    }
  }
  private iteration = (event: AnimationEvent) => {
    // Re-seed only at invisible/neutral cycle boundaries, never in the frame loop.
    if (event.target instanceof SVGElement && this.root.dataset.active !== 'false' && this.root.dataset.reduced !== 'true') this.vary(event.target);
  };
  constructor(parent: SVGGElement, uid: string, readonly mini = false) {
    const svg = new DOMParser().parseFromString(artwork, 'image/svg+xml').documentElement;
    for (const node of svg.querySelectorAll('[id]')) node.id = uid + '-' + node.id;
    for (const node of svg.querySelectorAll('[fill],[clip-path],[mask]')) for (const name of ['fill', 'clip-path', 'mask']) {
      if (node.hasAttribute(name)) node.setAttribute(name, node.getAttribute(name)!.replace(/url\(#([^)]*)\)/g, `url(#${uid}-$1)`));
    }
    this.root = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    this.root.classList.add('weather-lighthouse');
    if (mini) this.root.classList.add('weather-lighthouse-mini');
    this.root.setAttribute('transform', `scale(${DEMI_VIEWBOX * 2 / 380}) translate(-380 -250)`);
    this.root.append(...Array.from(svg.childNodes)); parent.append(this.root);
    if (mini) {
      this.root.querySelector('.wl-tower')!.setAttribute('clip-path', `url(#${uid}-miniTowerBounds)`);
      // Shorten below the orange ring, then bring its lower edge down to the unchanged waterline.
      for (const layer of [this.root.querySelector('.wl-assembly')!, this.root.querySelector('.wl-beams')!.parentElement!]) {
        layer.setAttribute('transform', 'translate(0 102) ' + layer.getAttribute('transform'));
      }
    }
    this.eyes = [...this.root.querySelectorAll<SVGPathElement>('.wl-eyes path')];
    this.sun = this.root.querySelector('.wl-sun')!; this.moon = this.root.querySelector('.wl-moon')!;
    this.stars = this.root.querySelector('.wl-stars')!; this.companion = this.root.querySelector('.wl-companion')!;
    if (mini) {
      this.towerGradients = ['tower', 'enamel', 'redBand'].map(name => this.root.querySelector<SVGLinearGradientElement>(`[id="${uid}-${name}"]`)!);
      const face = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      face.classList.add('wl-sun-face'); face.append(...Array.from(this.sun.children)); this.sun.append(face);
      this.moon.querySelector('circle:not(.wl-glint)')?.remove();
      this.moonLight = this.moon.querySelector('path')!;
      this.moonLight.classList.add('wl-moon-light');
      this.moonLight.setAttribute('fill', '#f8e6ac');
      this.moonLight.setAttribute('d', '');
      const clip = document.createElementNS('http://www.w3.org/2000/svg', 'clipPath');
      clip.id = uid + '-moonPhase';
      this.moonClip = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      clip.append(this.moonClip); this.root.querySelector('defs')!.append(clip);
      this.moon.querySelector('.wl-glint')!.setAttribute('clip-path', `url(#${clip.id})`);
      for (const body of [this.sun, this.moon]) {
        const horizon = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        horizon.setAttribute('clip-path', `url(#${uid}-miniSkyBounds)`);
        body.before(horizon); horizon.append(body);
      }
    }
    this.hits = [...this.root.querySelectorAll<SVGElement>('[data-bot-hit]')];
    for (const hit of this.hits) hit.style.pointerEvents = 'visiblePainted';
    this.root.addEventListener('animationiteration', this.iteration as EventListener);
    this.stars.replaceChildren();
    for (let i = 0; i < 9; i++) {
      const star = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      star.classList.add('wl-star'); this.vary(star);
      const duration = this.random(4.5, 9);
      star.style.animationDuration = duration + 's'; star.style.animationDelay = -Math.random() * duration + 's';
      this.stars.append(star);
    }
    const clouds = this.root.querySelector('.wl-clouds')!;
    if (mini) clouds.setAttribute('fill', `url(#${uid}-miniCloud)`);
    const cloudPath = clouds.querySelector('path')!.getAttribute('d')!;
    clouds.replaceChildren();
    for (let i = 0; i < 4; i++) {
      const cloud = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      cloud.setAttribute('d', cloudPath); cloud.classList.add('wl-cloud-flight'); this.vary(cloud);
      const duration = this.mini ? this.random(16, 26) : this.random(22, 38);
      cloud.style.animationDuration = duration + 's'; cloud.style.animationDelay = -(i / 4) * duration + 's';
      if (this.mini) cloud.style.animationDirection = i % 2 ? 'reverse' : 'normal';
      clouds.append(cloud);
    }
    const wind = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    wind.classList.add('wl-wind');
    for (let i = 0; i < 3; i++) {
      const gust = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      gust.setAttribute('d', `M220 ${180+i*54}q45 -12 80 0t70 0`);
      gust.style.animationDuration = this.random(3, 6) + 's'; gust.style.animationDelay = -i * 1.4 + 's'; wind.append(gust);
    }
    this.root.insertBefore(wind, clouds);
    const lightning = this.root.querySelector<SVGElement>('.wl-storm path')!;
    lightning.style.animationDuration = this.random(8, 14) + 's'; lightning.style.animationDelay = '-3s';
    const heat = this.root.querySelector('.wl-heat')!;
    for (let i = 0; i < 4; i++) {
      const wisp = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      wisp.setAttribute('d', 'M0 0C-14-10 14-21 0-32S-7-49 0-60');
      wisp.classList.add('wl-heat-wisp'); this.vary(wisp);
      const duration = this.random(4, 7);
      wisp.style.animationDuration = duration + 's'; wisp.style.animationDelay = -(i / 4) * duration + 's';
      heat.append(wisp);
    }
    this.root.querySelectorAll<SVGElement>('.wl-waves path').forEach((wave, index) => {
      if (mini) {
        const animation = wave.animate(waterFrames(index, Math.random()), {
          duration: 4400 + index * 1200 + this.random(0, 600), iterations: Infinity,
          easing: 'linear', delay: -Math.random() * 4000,
        });
        this.waterAnimations.push(animation);
        return;
      }
      // Randomise once, not every frame: gentle, independent layers without jitter.
      const duration = 5 + index * 1.3 + Math.random() * 2;
      wave.style.animationDuration = duration + 's';
      wave.style.animationDelay = -Math.random() * duration + 's';
      this.vary(wave);
    });
    for (const [kind, count] of [['rain', 12], ['snow', 8]] as const) {
      const group = this.root.querySelector('.wl-' + kind)!;
      for (let i = 0; i < count; i++) {
        const node = document.createElementNS('http://www.w3.org/2000/svg', kind === 'rain' ? 'path' : 'circle');
        const x = 225 + i * (300 / count) + Math.random() * 12, y = this.random(190, 320);
        if (kind === 'rain') node.setAttribute('d', `M${x} ${y}l-5 13`);
        else { node.setAttribute('cx', String(x)); node.setAttribute('cy', String(y)); node.setAttribute('r', '3'); }
        const duration = kind === 'rain' ? this.random(.9, 1.8) : this.random(3, 5.5);
        node.style.animationDuration = duration + 's'; node.style.animationDelay = -Math.random() * duration + 's'; group.append(node);
      }
    }
  }
  setActive(active: boolean, reduced: boolean) {
    this.root.dataset.active = String(active); this.root.dataset.reduced = String(reduced);
    for (const animation of this.waterAnimations) {
      if (active && !reduced) animation.play(); else animation.pause();
    }
  }
  setWeather(scene: WeatherScene) {
    this.root.dataset.weather = scene.weather; this.root.dataset.phase = scene.phase;
    if (this.mini) {
      const path = moonPath(scene.lunarPhase ?? NaN);
      if (this.moonLight?.getAttribute('d') !== path) {
        this.moonLight?.setAttribute('d', path);
        this.moonClip?.setAttribute('d', path);
      }
      const day = scene.lunarDay === undefined ? '' : String(scene.lunarDay);
      if (this.moon.dataset.lunarDay !== day) this.moon.dataset.lunarDay = day;
    }
    if (this.mini) for (const [name, value] of Object.entries(miniLighting(scene))) {
      const key = '--wl-mini-' + name;
      if (this.root.style.getPropertyValue(key) !== value) this.root.style.setProperty(key, value);
    }
    const sunY = this.mini && scene.solarCycle !== undefined ? 365 - Math.sin(Math.PI * scene.solarCycle) * 280 : scene.y;
    const moonY = this.mini && scene.rotation !== undefined ? 365 - Math.sin(scene.rotation * Math.PI / 180) * 280 : scene.moonY ?? scene.y;
    if (this.mini) {
      const night = scene.phase === 'night';
      const dx = (night ? scene.moonX ?? scene.x : scene.x) - 380;
      const dy = (night ? moonY : sunY) - 275;
      const length = Math.hypot(dx, dy) || 1;
      const light = { x1: .5 + dx / length / 2, y1: .5 + dy / length / 2,
        x2: .5 - dx / length / 2, y2: .5 - dy / length / 2 };
      // Move the lit face with the sky light; the opposite face keeps its material shadow.
      for (const gradient of this.towerGradients) for (const [key, value] of Object.entries(light)) {
        const coordinate = value.toFixed(4);
        if (gradient.getAttribute(key) !== coordinate) gradient.setAttribute(key, coordinate);
      }
    }
    const position = `translate(${scene.x.toFixed(3)}px, ${sunY.toFixed(3)}px)`;
    const moonPosition = `translate(${(scene.moonX ?? scene.x).toFixed(3)}px, ${moonY.toFixed(3)}px)`;
    this.sun.style.transform = position; this.moon.style.transform = moonPosition; this.companion.style.transform = moonPosition;
    this.stars.style.transform = `rotate(${(scene.rotation || 0).toFixed(3)}deg)`;
  }
  render(frame: BotFrame) {
    // Only painted tower pieces receive clicks; the sky, rays and square canvas do not.
    const matrices = frame.eyes.map(eye => eye.matrix.slice(7, -1).split(',').map(Number));
    const gaze = matrices.length ? 18 * Math.tanh(matrices.reduce((sum, matrix) => sum + matrix[4]!, 0) / matrices.length / 30) : 0;
    this.eyes.forEach((path, index) => {
      const eye = frame.eyes[index], display = eye ? '' : 'none';
      if (path.style.display !== display) path.style.display = display;
      if (!eye) return;
      const matrix = matrices[index]!;
      // Keep the existing blink/expression, but constrain gaze to the lamp's glass.
      matrix[4] = (index === 0 ? -28 : 28) + gaze; matrix[5] = 18 * Math.tanh(matrix[5]! / 22);
      for (const [key, value] of Object.entries({ d: eye.d, transform: `matrix(${matrix.join(',')})`, opacity: String(eye.alpha) })) {
        if (path.getAttribute(key) !== value) path.setAttribute(key, value);
      }
    });
  }
  destroy() {
    this.waterAnimations.forEach(animation => animation.cancel());
    this.root.removeEventListener('animationiteration', this.iteration as EventListener); this.root.remove();
  }
}
