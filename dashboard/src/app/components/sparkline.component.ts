import { Component, Input } from '@angular/core';

@Component({
  selector: 'app-sparkline',
  standalone: true,
  template: `
    <svg [attr.viewBox]="'0 0 ' + ancho + ' ' + alto" class="spark"
         preserveAspectRatio="none">
      @for (y of [0.25, 0.5, 0.75]; track y) {
        <line [attr.x1]="0" [attr.x2]="ancho"
              [attr.y1]="alto * y" [attr.y2]="alto * y"
              class="grid-line" />
      }
      @if (puntos.length > 1) {
        <polygon [attr.points]="areaPuntos"
                 [attr.fill]="color" fill-opacity="0.12" />
      }
      @if (puntos.length > 1) {
        <polyline [attr.points]="lineaPuntos"
                  fill="none" [attr.stroke]="color" stroke-width="2"
                  stroke-linejoin="round" stroke-linecap="round" />
      }
    </svg>
  `,
  styles: [`
    .spark { width: 100%; height: 64px; display: block; }
    .grid-line { stroke: #e4e7ec; stroke-width: 1; stroke-dasharray: 3 4; }
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
    const n = this.datos.length;
    return this.datos.map((v, i) => ({
      x: n === 1 ? 0 : (i / (n - 1)) * this.ancho,
      y: this.alto - (Math.min(v, 100) / 100) * this.alto,
    }));
  }

  get lineaPuntos(): string {
    return this.puntos.map(p => `${p.x},${p.y}`).join(' ');
  }

  get areaPuntos(): string {
    const p = this.puntos;
    if (p.length < 2) return '';
    return `${p[0].x},${this.alto} ` +
           p.map(pt => `${pt.x},${pt.y}`).join(' ') +
           ` ${p[p.length - 1].x},${this.alto}`;
  }
}
