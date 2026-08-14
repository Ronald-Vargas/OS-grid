import { Component, computed, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { GridService, NodoEstado } from '../services/grid.service';
import { SparklineComponent } from './sparkline.component';

/**
 * Dashboard principal de OS Grid. Muestra:
 *   - barra superior con conexión y progreso global de la misión
 *   - una tarjeta por nodo con CPU/RAM en vivo y gráfica de líneas
 *   - leaderboard final cuando la misión termina
 *
 * Todo se alimenta de los signals de GridService, que a su vez vienen del
 * WebSocket del coordinador. El componente no tiene lógica de red: solo
 * presenta el estado.
 */
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
  readonly mision = this.grid.mision;

  // Lista de nodos ordenada por nombre, para render estable.
  readonly nodos = computed(() =>
    Array.from(this.grid.nodos().values())
      .sort((a, b) => a.nombre.localeCompare(b.nombre))
  );

  // Progreso global en porcentaje.
  readonly progreso = computed(() => {
    const m = this.mision();
    return m.total ? Math.round((m.completados / m.total) * 100) : 0;
  });

  // Leaderboard ordenado por tiempo promedio (menor = mejor).
  readonly leaderboard = computed(() =>
    [...this.mision().porSo].sort((a, b) => a.tiempoProm - b.tiempoProm)
  );

  conectar(): void {
    this.grid.conectar(this.host());
  }

  /** Color asignado a cada SO, para mantener consistencia visual. */
  colorSO(os: string): string {
    const c: Record<string, string> = {
      'Windows': '#2563eb',  // azul
      'Linux': '#16a34a',    // verde
      'macOS': '#e05a4f',    // coral
    };
    return c[os] ?? '#6b7280';
  }

  iconoMedalla(i: number): string {
    return ['1', '2', '3'][i] ?? `${i + 1}`;
  }

  trackNodo(_: number, n: NodoEstado): string {
    return n.nombre;
  }
}
