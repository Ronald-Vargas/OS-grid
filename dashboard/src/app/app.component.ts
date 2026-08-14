import { Component, OnInit, inject, signal } from '@angular/core';
import { DashboardComponent } from './components/dashboard.component';
import { InteractionComponent } from './components/interaction.component';
import { GridService } from './services/grid.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [DashboardComponent, InteractionComponent],
  template: `
    <div class="tab-bar">
      <button [class.active]="tab() === 'panel'" (click)="tab.set('panel')">Panel principal</button>
      <button [class.active]="tab() === 'interaccion'" (click)="tab.set('interaccion')">⚡ Interacción SO</button>
    </div>
    @if (tab() === 'panel') {
      <app-dashboard />
    } @else {
      <app-interaction />
    }
  `,
  styles: [`
    .tab-bar { display: flex; background: #fff; border-bottom: 1px solid #e4e7ec; padding: 0 16px; }
    button { padding: 10px 20px; border: none; background: transparent; font-size: 13px; font-weight: 600; color: #667085; border-bottom: 2px solid transparent; margin-bottom: -1px; cursor: pointer; }
    button.active { color: #2563eb; border-bottom-color: #2563eb; }
    button:hover:not(.active) { color: #344054; }
  `]
})
export class AppComponent implements OnInit {
  tab = signal<'panel' | 'interaccion'>('panel');
  private grid = inject(GridService);

  ngOnInit(): void {
    // Auto-conectar usando el host de la URL actual.
    // Si accedés por http://100.70.74.9:8000/ conecta a esa IP.
    // Si accedés por http://localhost:8000/ conecta a localhost.
    const host = window.location.hostname || 'localhost';
    this.grid.conectar(host);
  }
}
