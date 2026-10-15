import artwork from './weather-lighthouse.svg?raw';
import type { BotFrame } from './engine/engine';
import { DEMI_VIEWBOX } from './engine/repere';
import type { WeatherScene } from '../../botWeather';
import './weatherLighthouse.css';

export class WeatherLighthouse {
  readonly root: SVGGElement;
  private eyes: SVGPathElement[];
  private sun: SVGGElement;
  private moon: SVGGElement;
  private stars: SVGGElement;
  private companion: SVGGElement;
  private hits: SVGElement[];
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
      node.style.setProperty('--wl-cloud-opacity', this.random(.45, .8).toFixed(2));
    } else if (node.closest('.wl-waves')) {
      node.style.setProperty('--wl-swell', this.random(.8, 4).toFixed(2) + 'px');
      node.style.setProperty('--wl-wave-height', this.random(.95, 1.3).toFixed(2));
    }
  }
  private iteration = (event: AnimationEvent) => {
    // Re-seed only at invisible/neutral cycle boundaries, never in the frame loop.
    if (event.target instanceof SVGElement && this.root.dataset.active !== 'false' && this.root.dataset.reduced !== 'true') this.vary(event.target);
  };
  constructor(parent: SVGGElement, uid: string) {
    const svg = new DOMParser().parseFromString(artwork, 'image/svg+xml').documentElement;
    for (const node of svg.querySelectorAll('[id]')) node.id = uid + '-' + node.id;
    for (const node of svg.querySelectorAll('[fill],[clip-path]')) for (const name of ['fill', 'clip-path']) {
      if (node.hasAttribute(name)) node.setAttribute(name, node.getAttribute(name)!.replace(/url\(#([^)]*)\)/g, `url(#${uid}-$1)`));
    }
    this.root = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    this.root.classList.add('weather-lighthouse');
    this.root.setAttribute('transform', `scale(${DEMI_VIEWBOX * 2 / 380}) translate(-380 -250)`);
    this.root.append(...Array.from(svg.childNodes)); parent.append(this.root);
    this.eyes = [...this.root.querySelectorAll<SVGPathElement>('.wl-eyes path')];
    this.sun = this.root.querySelector('.wl-sun')!; this.moon = this.root.querySelector('.wl-moon')!;
    this.stars = this.root.querySelector('.wl-stars')!; this.companion = this.root.querySelector('.wl-companion')!;
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
    const cloudPath = clouds.querySelector('path')!.getAttribute('d')!;
    clouds.replaceChildren();
    for (let i = 0; i < 4; i++) {
      const cloud = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      cloud.setAttribute('d', cloudPath); cloud.classList.add('wl-cloud-flight'); this.vary(cloud);
      const duration = this.random(22, 38);
      cloud.style.animationDuration = duration + 's'; cloud.style.animationDelay = -(i / 4) * duration + 's';
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
    this.root.querySelectorAll<SVGElement>('.wl-waves path').forEach((wave, index) => {
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
  }
  setWeather(scene: WeatherScene) {
    this.root.dataset.weather = scene.weather; this.root.dataset.phase = scene.phase;
    const position = `translate(${scene.x.toFixed(3)}px, ${scene.y.toFixed(3)}px)`;
    const moonPosition = `translate(${(scene.moonX ?? scene.x).toFixed(3)}px, ${(scene.moonY ?? scene.y).toFixed(3)}px)`;
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
  destroy() { this.root.removeEventListener('animationiteration', this.iteration as EventListener); this.root.remove(); }
}
