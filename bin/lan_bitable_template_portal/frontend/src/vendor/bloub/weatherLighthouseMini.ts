import { mixHex } from './engine/skins';
import type { WeatherScene } from '../../botWeather';

export function moonPath(phase: number, radius = 17): string {
  if (!Number.isFinite(phase) || phase < 0 || phase > 1) return '';
  const side = phase <= .5 ? 1 : -1;
  const edge = side * Math.cos(phase * Math.PI * 2);
  const width = (Math.abs(edge) * radius).toFixed(3);
  // The outer limb is a semicircle; the inner limb is the projected terminator ellipse.
  return `M0 ${-radius}A${radius} ${radius} 0 0 ${side > 0 ? 1 : 0} 0 ${radius}`
    + `A${width} ${radius} 0 0 ${edge >= 0 ? 0 : 1} 0 ${-radius}Z`;
}

// Cycle 0 is local sunrise, 1 is sunset, 2 is the next sunrise.
const lightStops: [number, ...string[]][] = [
  [0,    '#ffe3b6', '#9a92ab', '#ffcf91', '#e88687', '#fff0bc', '#ed784a'],
  [.12,  '#fff3da', '#b5cbdc', '#f6eeee', '#a6c0d4', '#fff8d7', '#f3b757'],
  [.5,   '#f8fbfd', '#b8ccdf', '#eef5fa', '#9dbbd0', '#fffdf0', '#efb94d'],
  [.8,   '#ffe9c8', '#a5aec9', '#f9dfb4', '#c3a5bd', '#fff3b3', '#f7a34b'],
  [.98,  '#ffd09a', '#8e7e9f', '#ffc570', '#d45e86', '#ffe68a', '#e6603d'],
  [1.06, '#bca3be', '#4b577e', '#d68ca0', '#63638e', '#ffc06e', '#cb5147'],
  [1.2,  '#7b8ca6', '#344969', '#677c9a', '#303e63', '#f1bb8b', '#ca6e63'],
  [1.6,  '#596d88', '#273955', '#4e6686', '#233451', '#edbf94', '#cc7c72'],
  [1.88, '#8c91ad', '#4c5279', '#b49bbd', '#62608b', '#f3c79f', '#e58977'],
  [2,    '#ffe3b6', '#9a92ab', '#ffcf91', '#e88687', '#fff0bc', '#ed784a'],
];

export function miniLighting(scene: WeatherScene): Record<string, string> {
  const cycle = scene.solarCycle ?? ({ dawn: 0, day: .5, dusk: .98, night: 1.5 }[scene.phase] ?? .5);
  const index = lightStops.findIndex(stop => stop[0] >= cycle);
  const right = lightStops[index < 0 ? lightStops.length - 1 : index]!;
  const left = lightStops[Math.max(0, index - 1)]!;
  const fraction = right[0] === left[0] ? 0 : Math.max(0, Math.min(1, (cycle - left[0]) / (right[0] - left[0])));
  const blend = fraction * fraction * (3 - 2 * fraction);
  const color = (i: number) => mixHex(left[i] as string, right[i] as string, blend);
  const lit = color(1), shade = color(2), cloudTop = color(3), cloudBottom = color(4);
  const daylight = Math.max(0, Math.sin(Math.PI * cycle));
  const band = mixHex('#8a4a4c', '#ff9068', Math.min(1, daylight + .35));
  return {
    'tower-start': lit, 'tower-mid': mixHex(lit, shade, .28), 'tower-end': shade,
    'enamel-start': mixHex('#35548c', lit, .28), 'enamel-mid': mixHex('#20365f', shade, .35),
    'enamel-end': mixHex('#152544', shade, .2),
    'band-start': mixHex(band, lit, .3), 'band-mid': band, 'band-end': mixHex(band, shade, .45),
    'cloud-start': cloudTop, 'cloud-mid': mixHex(cloudTop, cloudBottom, .4), 'cloud-end': cloudBottom,
    'sun-start': color(5), 'sun-mid': mixHex(color(5), color(6), .45), 'sun-end': color(6),
    'sun-aura': mixHex(color(5), color(6), .55),
    'water-front': mixHex('#2f687f', mixHex('#58b7dd', cloudBottom, .35), daylight),
    'water-back': mixHex('#48647f', mixHex('#a8ddf2', cloudTop, .3), daylight),
    'sun-size': (1 + .2 * (1 - Math.abs(Math.sin(Math.PI * cycle))) ** 3).toFixed(3),
  };
}

export function waterFrames(layer: number, seed: number): Keyframe[] {
  const baseline = [367, 383, 397][layer]!;
  const amplitude = [10, 7, 4.5][layer]!;
  const phase = seed * Math.PI * 2, direction = layer % 2 ? -1 : 1;
  const waveNumber = Math.PI * 2 / (110 + layer * 24);
  const pathAt = (t: number) => {
    const swell = amplitude * (.8 + .2 * Math.sin(Math.PI * 2 * t + phase));
    const wave = (x: number) => {
      const a = waveNumber * x - direction * Math.PI * 2 * t + phase;
      const b = waveNumber * .57 * x + Math.PI * 4 * t - phase;
      return { y: baseline + swell * (Math.sin(a) + .32 * Math.sin(b)),
        slope: swell * waveNumber * (Math.cos(a) + .32 * .57 * Math.cos(b)) };
    };
    let path = 'M190 ' + wave(190).y.toFixed(2);
    // Cubic slopes make traveling crests smooth without a per-frame JavaScript loop.
    for (let x = 190; x < 570; x += 19) {
      const from = wave(x), to = wave(x + 19), step = 19 / 3;
      path += `C${(x + step).toFixed(2)} ${(from.y + from.slope * step).toFixed(2)} ${(x + 19 - step).toFixed(2)} ${(to.y - to.slope * step).toFixed(2)} ${x + 19} ${to.y.toFixed(2)}`;
    }
    return `path("${path}")`;
  };
  const frames = Array.from({ length: 16 }, (_, i) => ({ d: pathAt(i / 16), offset: i / 16 }));
  return [...frames, { ...frames[0]!, offset: 1 }];
}
