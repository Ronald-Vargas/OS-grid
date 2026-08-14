import { Component, computed, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { GridService, NodoEstado } from '../services/grid.service';
import { SparklineComponent } from './sparkline.component';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule, FormsModule, SparklineComponent],
  templateUrl: './dashboard.component.html',
  styleUrl: './dashboard.component.css',
})
export class DashboardComponent {
  private grid = inject(GridService);

  host = signal('localhost');
  readonly conectado = this.grid.conectado;
  readonly mision    = this.grid.mision;
  readonly esperando = this.grid.esperando;

  readonly nodos = computed(() =>
    Array.from(this.grid.nodos().values())
      .sort((a, b) => a.nombre.localeCompare(b.nombre))
  );

  readonly progreso = computed(() => {
    const m = this.mision();
    return m.total ? Math.round((m.completados / m.total) * 100) : 0;
  });

  readonly leaderboard = computed(() =>
    [...this.mision().porSo].sort((a, b) => a.tiempoProm - b.tiempoProm)
  );

  conectar(): void {
    this.grid.conectar(this.host());
  }

  colorSO(os: string): string {
    return this.grid.colorSO(os);
  }

  iconoMedalla(i: number): string {
    return ['1', '2', '3'][i] ?? `${i + 1}`;
  }

  trackNodo(_: number, n: NodoEstado): string {
    return n.nombre;
  }
}
