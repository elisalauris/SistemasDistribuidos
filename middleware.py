import signal
import socket
import sys
import threading
import time
import traceback
from datetime import datetime
from typing import Optional

# Importaciones absolutas para cuando se ejecuta directamente
from server_manager import ServerManager
from distributed_sort import DistributedSorter
from client_handler import ClientHandler
from utils import logger, get_local_ip


class DistributedMiddleware:
    
    def __init__(self, port: int = 60000):
        self.port = port
        self.ip = get_local_ip()
        self.start_time = time.time()
        self.running = True
        
        self.server_manager = ServerManager()
        self.distributed_sorter = DistributedSorter(self.server_manager)
        self.client_handler = ClientHandler(self.server_manager, self.distributed_sorter)
    
    def monitor_servers(self) -> None:
        while self.running:
            try:
                # Checar  servidores cada 30 segundos
                time.sleep(30)
                if self.server_manager.servers:
                    self.server_manager.update_servers_status()
            except Exception as e:
                logger.error(f"Error en monitoreo de servidores: {e}")
    
    def shutdown(self, server_socket: Optional[socket.socket] = None) -> None:

        logger.info("Apagando el middleware distribuido...")
        self.running = False
        
        # Imprimir stats finales
        print("\n" + "=" * 60)
        print("📊 ESTADÍSTICAS 📊".center(60))
        print("=" * 60)
        uptime = time.time() - self.start_time
        hours, remainder = divmod(uptime, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print(f"⏱️  Tiempo activo: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        print(f"🔢 Operaciones completadas: {self.client_handler.successful_operations}")
        print(f"❌ Operaciones fallidas: {self.client_handler.total_operations - self.client_handler.successful_operations}")
        print(f"📈 Bytes procesados: {self.distributed_sorter.stats['bytes_processed']:,}")
        print(f"📊 Números ordenados: {self.distributed_sorter.stats['numbers_sorted']:,}")
        print("=" * 60 + "\n")
        
        if server_socket:
            server_socket.close()
    
    def start(self) -> None:
        """Inicia el middleware distribuido."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind(('0.0.0.0', self.port))
            server_socket.listen(10)
            
            # Registrar handlers para señales
            signal.signal(signal.SIGINT, lambda sig, frame: self.shutdown(server_socket))
            signal.signal(signal.SIGTERM, lambda sig, frame: self.shutdown(server_socket))
            
            # Banner de inicio
            print("\n" + "=" * 60)
            print("🚀 LAB 5 / ORDENAMIENTO DISTRIBUIDO 🚀".center(60))
            print("=" * 60)
            print(f"⏰ Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🌐 Escuchando en: {self.ip}:{self.port}")
            print(f"🔗 IP pa las conexiones externas: {self.ip}")
            print("=" * 60 + "\n")
            
            # Thread para monitoreo periodic de los servidores
            monitor_thread = threading.Thread(target=self.monitor_servers, daemon=True)
            monitor_thread.start()
 
            logger.info(f"Middleware distribuido iniciado en {self.ip}:{self.port}")
            
            while self.running:
                try:
                    server_socket.settimeout(1.0)
                    try:
                        client_socket, address = server_socket.accept()
                        logger.info(f"Nueva conexión desde {address}")
                        threading.Thread(
                            target=self.client_handler.handle_client, 
                            args=(client_socket, address), 
                            daemon=True
                        ).start()
                    except socket.timeout:
                        continue
                except Exception as e:
                    if self.running:  
                        logger.error(f"Error aceptando conexión: {e}")
                
        except OSError as e:
            logger.critical(f"Error al iniciar el servidor: {e}")
            if e.errno == 98:  # Address already in use
                logger.critical(f"El puerto {self.port} ya está en uso. Intente con otro puerto.")
            sys.exit(1)
        finally:
            server_socket.close()
            logger.info("Middleware distribuido detenido")


def main():
    try:

        middleware = DistributedMiddleware()
        middleware.start()
    except KeyboardInterrupt:
        print("\nMiddleware detenido manualmente")
    except Exception as e:
        print(f"Error crítico: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()