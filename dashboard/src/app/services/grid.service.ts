import { Injectable, signal } from '@angular/core';

export interface NodoEstado {
  nombre: string; os: string;
  cpuProc: number; cpuSys: number; ramMb: number;
  chunksHechos: number; activo: boolean; historialCpu: number[];
}
export interface FilaSO {
  so: string; chunks: number; tiempoProm: number; cpuProm: number; ramProm: number;
}
export interface EstadoMision {
  completa: boolean; encontrado: number | null; porSo: FilaSO[];
  completados: number; total: number;
}
export interface EventoLog {
  hora: string; texto: string; tipo: 'info' | 'chunk' | 'metric' | 'done' | 'join';
}

@Injectable({ providedIn: 'root' })
export class GridService {
  readonly nodos      = signal<Map<string, NodoEstado>>(new Map());
  readonly conectado  = signal(false);
  readonly mision     = signal<EstadoMision>({ completa: false, encontrado: null, porSo: [], completados: 0, total: 0 });
  readonly esperando  = signal({ activo: false, conectados: 0, minimo: 0 });
  readonly configurando = signal(false);   // ← nuevo: panel de config visible
  readonly eventos    = signal<EventoLog[]>([]);

  private ws?: WebSocket;
  private readonly MAX_HISTORIAL = 30;
  private readonly MAX_EVENTOS   = 60;

  conectar(host = 'localhost', puerto = 8000): void {
    const url = `ws://${host}:${puerto}/ws/dashboard`;
    this.ws = new WebSocket(url);
    this.ws.onopen  = () => { this.conectado.set(true);  this.log('Conectado al coordinador', 'info'); }
    this.ws.onclose = () => { this.conectado.set(false); this.log('Desconectado', 'info'); }
    this.ws.onerror = () => this.conectado.set(false);
    this.ws.onmessage = (ev) => this.procesarEvento(JSON.parse(ev.data));
  }

  desconectar(): void { this.ws?.close(); }

  /** Envía la configuración al coordinador para iniciar la misión. */
  arrancarMision(tipo: string, intensidad: string): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    this.ws.send(JSON.stringify({ type: 'start_mission', tipo, intensidad }));
    this.configurando.set(false);
  }

  private procesarEvento(ev: any): void {
    switch (ev.type) {
      case 'snapshot':          this.aplicarSnapshot(ev); break;
      case 'waiting':           this.onWaiting(ev); break;
      case 'ready_to_configure': this.onReadyToConfigure(ev); break;
      case 'mission_starting':  this.onMissionStarting(ev); break;
      case 'worker_join':       this.agregarNodo(ev); break;
      case 'metrics':           this.actualizarMetricas(ev); break;
      case 'chunk_done':        this.chunkCompletado(ev); break;
      case 'mission_complete':  this.misionCompleta(ev); break;
    }
  }

  private aplicarSnapshot(ev: any): void {
    const mapa = new Map<string, NodoEstado>();
    for (const w of ev.workers ?? []) mapa.set(w.nombre, this.nodoVacio(w.nombre, w.os, w.chunks_hechos));
    this.nodos.set(mapa);
    this.mision.update(m => ({ ...m, total: ev.chunks_totales, completados: ev.completados }));
    const n = (ev.workers ?? []).length;
    const fase = ev.fase ?? "esperando";
    if (fase === "listo") {
      this.esperando.set({ activo: false, conectados: n, minimo: ev.min_workers });
      this.configurando.set(true);
    } else if (fase === "esperando") {
      this.esperando.set({ activo: true, conectados: n, minimo: ev.min_workers });
      this.configurando.set(false);
    } else {
      this.esperando.set({ activo: false, conectados: n, minimo: ev.min_workers });
      this.configurando.set(false);
    }
  }

  private onWaiting(ev: any): void {
    this.esperando.set({ activo: true, conectados: ev.conectados, minimo: ev.minimo });
    this.configurando.set(false);
    this.log(`Workers conectados: ${ev.conectados}/${ev.minimo}`, 'join');
  }

  private onReadyToConfigure(ev: any): void {
    this.esperando.set({ activo: false, conectados: ev.conectados, minimo: ev.minimo ?? 0 });
    this.configurando.set(true);
    // Resetear misión si era de corrida anterior
    this.mision.set({ completa: false, encontrado: null, porSo: [], completados: 0, total: 0 });
    this.log(`¡${ev.conectados} workers listos! Configurá la misión.`, 'join');
  }

  private onMissionStarting(ev: any): void {
    this.configurando.set(false);
    this.mision.update(m => ({ ...m, total: ev.chunks_totales }));
    this.log(`Misión iniciada: ${ev.tipo} / ${ev.intensidad}`, 'info');
  }

  private agregarNodo(ev: any): void {
    const mapa = new Map(this.nodos());
    if (!mapa.has(ev.nombre)) mapa.set(ev.nombre, this.nodoVacio(ev.nombre, ev.os, 0));
    this.nodos.set(mapa);
    this.mision.update(m => ({ ...m, total: ev.chunks_totales ?? m.total }));
    this.log(`${ev.nombre} (${ev.os}) conectado`, 'join');
  }

  private actualizarMetricas(ev: any): void {
    const mapa = new Map(this.nodos());
    const nodo = mapa.get(ev.nombre);
    if (nodo) {
      const hist = [...nodo.historialCpu, ev.cpu_proc].slice(-this.MAX_HISTORIAL);
      mapa.set(ev.nombre, { ...nodo, cpuProc: ev.cpu_proc, cpuSys: ev.cpu_sys, ramMb: ev.ram_mb, activo: true, historialCpu: hist });
      this.nodos.set(mapa);
    }
  }

  private chunkCompletado(ev: any): void {
    this.esperando.set({ activo: false, conectados: 0, minimo: 0 });
    this.configurando.set(false);
    const mapa = new Map(this.nodos());
    const nodo = mapa.get(ev.nombre);
    if (nodo) mapa.set(ev.nombre, { ...nodo, chunksHechos: ev.chunks_hechos, activo: false });
    this.nodos.set(mapa);
    this.mision.update(m => ({ ...m, completados: ev.completados, total: ev.total }));
    this.log(`${ev.nombre} completó chunk #${ev.chunk_id} en ${ev.tiempo?.toFixed(2)}s`, 'chunk');
  }

  private misionCompleta(ev: any): void {
    const porSo: FilaSO[] = (ev.por_so ?? []).map((f: any) => ({
      so: f.so, chunks: f.chunks, tiempoProm: f.tiempo_prom, cpuProm: f.cpu_prom, ramProm: f.ram_prom,
    }));
    this.mision.update(m => ({ ...m, completa: true, encontrado: ev.encontrado, porSo }));
    const mapa = new Map(this.nodos());
    for (const [k, v] of mapa) mapa.set(k, { ...v, activo: false });
    this.nodos.set(mapa);
    this.log(`¡MISIÓN COMPLETA! Número: ${ev.encontrado}`, 'done');
  }

  private log(texto: string, tipo: EventoLog['tipo']): void {
    const hora = new Date().toLocaleTimeString('es-CR', { hour12: false });
    this.eventos.update(e => [{ hora, texto, tipo }, ...e].slice(0, this.MAX_EVENTOS));
  }

  private nodoVacio(nombre: string, os: string, chunks: number): NodoEstado {
    return { nombre, os, cpuProc: 0, cpuSys: 0, ramMb: 0, chunksHechos: chunks, activo: false, historialCpu: [] };
  }

  colorSO(os: string): string {
    return ({ Windows: '#2563eb', Linux: '#16a34a', macOS: '#e05a4f' })[os] ?? '#6b7280';
  }
}
