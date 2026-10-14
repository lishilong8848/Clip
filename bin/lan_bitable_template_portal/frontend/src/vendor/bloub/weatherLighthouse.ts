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
    this.root.querySelectorAll<SVGElement>('.wl-waves path').forEach((wave, index) => {
      // Randomise once, not every frame: gentle, independent layers without jitter.
      const duration = 5 + index * 1.3 + Math.random() * 2;
      wave.style.animationDuration = duration + 's';
      wave.style.animationDelay = -Math.random() * duration + 's';
      wave.style.setProperty('--wl-swell', (1 + Math.random() * 1.3).toFixed(2) + 'px');
    });
    for (const [kind, count] of [['rain', 12], ['snow', 8]] as const) {
      const group = this.root.querySelector('.wl-' + kind)!;
      for (let i = 0; i < count; i++) {
        const node = document.createElementNS('http://www.w3.org/2000/svg', kind === 'rain' ? 'path' : 'circle');
        const x = 238 + i * (284 / count), y = 220 + (i * 37 % 130);
        if (kind === 'rain') node.setAttribute('d', `M${x} ${y}l-5 13`);
        else { node.setAttribute('cx', String(x)); node.setAttribute('cy', String(y)); node.setAttribute('r', '3'); }
        node.style.animationDelay = -i * .23 + 's'; group.append(node);
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
    const gaze = matrices.length ? 12 * Math.tanh(matrices.reduce((sum, matrix) => sum + matrix[4]!, 0) / matrices.length / 60) : 0;
    this.eyes.forEach((path, index) => {
      const eye = frame.eyes[index], display = eye ? '' : 'none';
      if (path.style.display !== display) path.style.display = display;
      if (!eye) return;
      const matrix = matrices[index]!;
      // Keep the existing blink/expression, but constrain gaze to the lamp's glass.
      matrix[4] = (index === 0 ? -28 : 28) + gaze; matrix[5] = 15 * Math.tanh(matrix[5]! / 30);
      for (const [key, value] of Object.entries({ d: eye.d, transform: `matrix(${matrix.join(',')})`, opacity: String(eye.alpha) })) {
        if (path.getAttribute(key) !== value) path.setAttribute(key, value);
      }
    });
  }
  destroy() { this.root.remove(); }
}
