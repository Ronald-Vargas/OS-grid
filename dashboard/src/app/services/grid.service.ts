import { Injectable, signal } from '@angular/core';

/**
 * Modelo del estado de un nodo worker en el dashboard.
 */
export interface NodoEstado {
  nombre: string;
  os: string;
  cpuProc: number;      // CPU del proceso (%)
  cpuSys: number;       // CPU del sistema (%)
  ramMb: number;        // RAM del proceso (MB)
  chunksHechos: number; // cuántos chunks completó
  activo: boolean;      // si está procesando ahora mismo
  historialCpu: number[]; // últimas lecturas de CPU para la gráfica
}

export interface FilaSO {
  so: string;
  chunks: number;
  tiempoProm: number;
  cpuProm: number;
  ramProm: number;
}

export interface EstadoMision {
  completa: boolean;
  encontrado: number | null;
  porSo: FilaSO[];
  completados: number;
  total: number;
}

/**
 * Servicio central del dashboard. Se conecta al canal /ws/dashboard del
 * coordinador y traduce los eventos que llegan en signals de Angular que
 * los componentes consumen reactivamente.
 *
 * No envía trabajo: solo escucha. El coordinador retransmite aquí todo lo
 * que ocurre en el clúster (workers que entran, métricas, chunks
 * completados, y el leaderboard final).
 */
@Injectable({ providedIn: 'root' })
export class GridService {
  // Signals que los componentes leen.
  readonly nodos = signal<Map<string, NodoEstado>>(new Map());
  readonly conectado = signal(false);
  readonly mision = signal<EstadoMision>({
    completa: false, encontrado: null, porSo: [],
    completados: 0, total: 0,
  });

  private ws?: WebSocket;
  private readonly MAX_HISTORIAL = 30; // puntos que guarda la gráfica

  /** Abre la conexión al coordinador. host ej: "localhost" o IP Tailscale. */
  conectar(host: string = 'localhost', puerto: number = 8000): void {
    const url = `ws://${host}:${puerto}/ws/dashboard`;
    this.ws = new WebSocket(url);

    this.ws.onopen = () => this.conectado.set(true);
    this.ws.onclose = () => this.conectado.set(false);
    this.ws.onerror = () => this.conectado.set(false);
    this.ws.onmessage = (ev) => this.procesarEvento(JSON.parse(ev.data));
  }

  desconectar(): void {
    this.ws?.close();
  }

  private procesarEvento(ev: any): void {
    switch (ev.type) {
      case 'snapshot':
        this.aplicarSnapshot(ev);
        break;
      case 'worker_join':
        this.agregarNodo(ev);
        break;
      case 'metrics':
        this.actualizarMetricas(ev);
        break;
      case 'chunk_done':
        this.chunkCompletado(ev);
        break;
      case 'mission_complete':
        this.misionCompleta(ev);
        break;
    }
  }

  private aplicarSnapshot(ev: any): void {
    const mapa = new Map<string, NodoEstado>();
    for (const w of ev.workers ?? []) {
      mapa.set(w.nombre, this.nodoVacio(w.nombre, w.os, w.chunks_hechos));
    }
    this.nodos.set(mapa);
    this.mision.update(m => ({
      ...m, total: ev.chunks_totales, completados: ev.completados,
    }));
  }

  private agregarNodo(ev: any): void {
    const mapa = new Map(this.nodos());
    if (!mapa.has(ev.nombre)) {
      mapa.set(ev.nombre, this.nodoVacio(ev.nombre, ev.os, 0));
    }
    this.nodos.set(mapa);
    this.mision.update(m => ({ ...m, total: ev.chunks_totales ?? m.total }));
  }

  private actualizarMetricas(ev: any): void {
    const mapa = new Map(this.nodos());
    const nodo = mapa.get(ev.nombre);
    if (nodo) {
      const hist = [...nodo.historialCpu, ev.cpu_proc].slice(-this.MAX_HISTORIAL);
      mapa.set(ev.nombre, {
        ...nodo,
        cpuProc: ev.cpu_proc,
        cpuSys: ev.cpu_sys,
        ramMb: ev.ram_mb,
        activo: true,
        historialCpu: hist,
      });
      this.nodos.set(mapa);
    }
  }

  private chunkCompletado(ev: any): void {
    const mapa = new Map(this.nodos());
    const nodo = mapa.get(ev.nombre);
    if (nodo) {
      mapa.set(ev.nombre, {
        ...nodo,
        chunksHechos: ev.chunks_hechos,
        activo: false,
      });
      this.nodos.set(mapa);
    }
    this.mision.update(m => ({
      ...m, completados: ev.completados, total: ev.total,
    }));
  }

  private misionCompleta(ev: any): void {
    const porSo: FilaSO[] = (ev.por_so ?? []).map((f: any) => ({
      so: f.so,
      chunks: f.chunks,
      tiempoProm: f.tiempo_prom,
      cpuProm: f.cpu_prom,
      ramProm: f.ram_prom,
    }));
    this.mision.update(m => ({
      ...m, completa: true, encontrado: ev.encontrado, porSo,
    }));
    // Marcar todos los nodos como inactivos al terminar.
    const mapa = new Map(this.nodos());
    for (const [k, v] of mapa) mapa.set(k, { ...v, activo: false });
    this.nodos.set(mapa);
  }

  private nodoVacio(nombre: string, os: string, chunks: number): NodoEstado {
    return {
      nombre, os,
      cpuProc: 0, cpuSys: 0, ramMb: 0,
      chunksHechos: chunks, activo: false,
      historialCpu: [],
    };
  }
}
