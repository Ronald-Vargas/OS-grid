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

  // Estado del coordinador
  readonly conectado    = this.grid.conectado;
  readonly mision       = this.grid.mision;
  readonly esperando    = this.grid.esperando;
  readonly configurando = this.grid.configurando;

  // Config seleccionada en el panel
  tipo        = signal<'hash' | 'sort' | 'primos'>('hash');
  intensidad  = signal<'ligero' | 'normal' | 'pesado'>('normal');

  // Descripciones de los tipos de tarea
  readonly tiposInfo = [
    { id: 'hash',   label: 'SHA-256',      desc: 'Búsqueda por fuerza bruta de un hash. CPU pura.' },
    { id: 'sort',   label: 'Ordenamiento', desc: 'Ordenar arrays grandes en memoria.' },
    { id: 'primos', label: 'Núm. primos',  desc: 'Contar primos en un rango (criba).' },
  ] as const;

  readonly intensidadesInfo = [
    { id: 'ligero', label: 'Ligero',  desc: 'Pocos chunks, rápido (~5-10s).' },
    { id: 'normal', label: 'Normal',  desc: 'Carga estándar (~15-30s).' },
    { id: 'pesado', label: 'Pesado',  desc: 'Máxima carga, más datos para el informe.' },
  ] as const;

  // Nodos y progreso
  readonly nodos = computed(() =>
    Array.from(this.grid.nodos().values()).sort((a, b) => a.nombre.localeCompare(b.nombre))
  );
  readonly progreso = computed(() => {
    const m = this.mision();
    return m.total ? Math.round((m.completados / m.total) * 100) : 0;
  });
  readonly leaderboard = computed(() =>
    [...this.mision().porSo].sort((a, b) => a.tiempoProm - b.tiempoProm)
  );

  conectar(): void  { this.grid.conectar(this.host()); }
  colorSO(os: string): string { return this.grid.colorSO(os); }
  iconoMedalla(i: number): string { return ['1', '2', '3'][i] ?? `${i + 1}`; }
  trackNodo(_: number, n: NodoEstado): string { return n.nombre; }

  arrancar(): void {
    this.grid.arrancarMision(this.tipo(), this.intensidad());
  }
}
