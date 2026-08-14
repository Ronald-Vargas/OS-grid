import { Component, Input } from '@angular/core';

/**
 * Mini gráfica de líneas (sparkline) que dibuja el historial de CPU de un
 * nodo. Hecha con SVG puro para no depender de librerías externas: es
 * liviana y se actualiza sola cuando cambia el array de entrada.
 */
@Component({
  selector: 'app-sparkline',
  standalone: true,
  template: `
    <svg [attr.viewBox]="'0 0 ' + ancho + ' ' + alto" class="spark"
         preserveAspectRatio="none">
      <!-- rejilla de fondo: líneas al 25/50/75% -->
      @for (y of [0.25, 0.5, 0.75]; track y) {
        <line [attr.x1]="0" [attr.x2]="ancho"
              [attr.y1]="alto * y" [attr.y2]="alto * y"
              class="grid-line" />
      }
      <!-- área bajo la curva -->
      @if (puntos.length > 1) {
        <polygon [attr.points]="areaPuntos"
                 [attr.fill]="color" fill-opacity="0.12" />
      }
      <!-- línea -->
      @if (puntos.length > 1) {
        <polyline [attr.points]="lineaPuntos"
                  fill="none" [attr.stroke]="color" stroke-width="2"
                  stroke-linejoin="round" stroke-linecap="round" />
      }
    </svg>
  `,
  styles: [`
    .spark { width: 100%; height: 64px; display: block; }
    .grid-line { stroke: var(--borde); stroke-width: 1; stroke-dasharray: 3 4; }
  `],
  imports: [],
})
export class SparklineComponent {
  @Input() datos: number[] = [];
  @Input() color = '#2563eb';

  readonly ancho = 200;
  readonly alto = 64;

  get puntos(): { x: number; y: number }[] {
    if (this.datos.length === 0) return [];
    const max = 100; // CPU en 0-100%
    const n = this.datos.length;
    return this.datos.map((v, i) => ({
      x: n === 1 ? 0 : (i / (n - 1)) * this.ancho,
      y: this.alto - (Math.min(v, max) / max) * this.alto,
    }));
  }

  get lineaPuntos(): string {
    return this.puntos.map(p => `${p.x},${p.y}`).join(' ');
  }

  get areaPuntos(): string {
    const p = this.puntos;
    if (p.length < 2) return '';
    const primero = p[0];
    const ultimo = p[p.length - 1];
    return `${primero.x},${this.alto} ` +
           p.map(pt => `${pt.x},${pt.y}`).join(' ') +
           ` ${ultimo.x},${this.alto}`;
  }
}
