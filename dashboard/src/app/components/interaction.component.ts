import { Component, computed, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { GridService, NodoEstado } from '../services/grid.service';

@Component({
  selector: 'app-interaction',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './interaction.component.html',
  styleUrl: './interaction.component.css',
})
export class InteractionComponent {
  private grid = inject(GridService);

  readonly conectado = this.grid.conectado;
  readonly mision    = this.grid.mision;
  readonly esperando = this.grid.esperando;
  readonly eventos   = this.grid.eventos;

  readonly nodos = computed(() =>
    Array.from(this.grid.nodos().values())
      .sort((a, b) => a.nombre.localeCompare(b.nombre))
  );

  readonly leaderboard = computed(() =>
    [...this.mision().porSo].sort((a, b) => a.tiempoProm - b.tiempoProm)
  );

  readonly progreso = computed(() => {
    const m = this.mision();
    return m.total ? Math.round((m.completados / m.total) * 100) : 0;
  });

  colorSO(os: string): string  { return this.grid.colorSO(os); }

  schedulerSO(os: string): string {
    return ({ Linux: 'Scheduler CFS/EEVDF', macOS: 'Scheduler XNU · Mach+BSD', Windows: 'Scheduler híbrido · NT kernel' })[os] ?? os;
  }

  trackNodo(_: number, n: NodoEstado): string { return n.nombre; }
  getX2(i: number, total: number): string { return ((100 / total) * (i + 0.5)) + "%"; }
}
