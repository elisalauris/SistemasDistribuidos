import time
import socket
import threading
from dataclasses import dataclass
from typing import List, Tuple, Dict

try:
    from .utils import logger
except ImportError:
    from utils import logger

@dataclass
class ServerInfo:
    """Info sobre un servidor"""
    ip: str
    port: int
    name: str
    active: bool = True
    last_response_time: float = 0.0  # tiempo de respuesta en segundos
    last_check_time: float = 0.0     # ultima verficacion de estado
    total_processed: int = 0         # total de elementos procesados
    
    def __str__(self) -> str:
        estado = "🟢 Activo" if self.active else "🔴 Inactivo"
        return f"{self.name} [{self.ip}:{self.port}] - {estado}"


class ServerManager:

    
    def __init__(self):
        self.servers: List[ServerInfo] = []
        self.servers_lock = threading.Lock()
    
    def is_server_alive(self, ip: str, port: int) -> bool:
        """
        Checar si un server está disponible.
        
        Args:
            ip: Dirección IP del servidor
            port: Puerto del servidor
            
        Returns:
            bool: True si el servidor está activo, False en caso contrario
        """
        if ip in ["127.0.0.1", "localhost"] or ip == socket.gethostbyname(socket.gethostname()):
            timeout = 1.0  # Timeout corto para conexiones locales
        else:
            timeout = 3.0  # Timeout largo para conexiones remotas
            
        try:
            with socket.create_connection((ip, port), timeout=timeout) as sock:
                # Conectado exitosamente
                return True
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            logger.debug(f"Error al verificar servidor {ip}:{port} - {type(e).__name__}: {e}")
            return False
    
    def update_servers_status(self) -> Tuple[int, int]:
        """
        Actualiza el estado de todos los servers registrados
        
        Returns:
            Tuple[int, int]: (servidores activos, servidores totales)
        """
        now = time.time()
        active_count = 0
        
        with self.servers_lock:
            for i, server in enumerate(self.servers):
                # Guardar estado anterior para detectar cambios
                was_active = server.active
                
                is_active = self.is_server_alive(server.ip, server.port)
                self.servers[i].active = is_active
                self.servers[i].last_check_time = now
                
                if is_active:
                    active_count += 1
                
                # Solo reportar cuando hay cambio de estado pa evitar el spam
                if was_active != is_active:
                    if is_active:
                        logger.info(f"✅ Servidor {server.name} [{server.ip}:{server.port}] ahora disponible")
                    else:
                        logger.warning(f"❌ Servidor {server.name} [{server.ip}:{server.port}] ahora no disponible")
            
            logger.info(f"Servidores activos: {active_count}/{len(self.servers)}")
            return active_count, len(self.servers)
    
    def register_server(self, data: Dict) -> Dict:
        """
        Registra un nuevo servidor o actualiza uno existente.
        
        Args:
            data: Diccionario con los datos del servidor (ip, port, name)
            
        Returns:
            Dict: Respuesta con el estado del registro
        """
        try:
            ip = data.get('ip')
            port = data.get('port')
            name = data.get('name')
            
            if not all([ip, port, name]):
                return {"status": "error", "mensaje": "Faltan campos obligatorios (ip, port, name)"}
            
            # Validar puerto
            if not isinstance(port, int) or port <= 0 or port > 65535:
                return {"status": "error", "mensaje": "Puerto inválido"}
                
            with self.servers_lock:
                # Verificar si el servidor ya existe
                for i, server in enumerate(self.servers):
                    if server.ip == ip and server.port == port:
                        self.servers[i] = ServerInfo(ip, port, name, True, time.time(), time.time())
                        logger.info(f"Servidor [{name}] reconectado [{ip}:{port}]")
                        break
                else:
                    # Agregar nuevo servidor
                    self.servers.append(ServerInfo(ip, port, name, True, time.time(), time.time()))
                    logger.info(f"✨ Nuevo servidor [{name}] conectado [{ip}:{port}]")
                    
            return {"status": "registered", "distribuido": True, "middleware_name": "Registrado con exito en el middleware;)"}
        except Exception as e:
            logger.error(f"Error al registrar servidor: {e}")
            return {"status": "error", "mensaje": f"Error interno: {str(e)}"}
    
    def get_active_servers(self) -> List[ServerInfo]:
        """
        Obtiene la lista de servidores activos.
        
        Returns:
            List[ServerInfo]: Lista de servidores activos
        """
        with self.servers_lock:
            return [s for s in self.servers if s.active]
    
    def update_server_stats(self, server_ip: str, server_port: int, 
                           processing_time: float, processed_items: int) -> None:
        """
        Actualiza las stats de un servidor despues de un procesamiento.
        
        Args:
            server_ip: IP del servidor
            server_port: Puerto del servidor
            processing_time: Tiempo de procesamiento en segundos
            processed_items: Número de elementos procesados
        """
        with self.servers_lock:
            for i, s in enumerate(self.servers):
                if s.ip == server_ip and s.port == server_port:
                    self.servers[i].last_response_time = processing_time
                    self.servers[i].total_processed += processed_items
                    break
    
    def mark_server_inactive(self, server_ip: str, server_port: int) -> None:
        """
        Marca un servidor como inactivo.
        
        Args:
            server_ip: IP del servidor
            server_port: Puerto del servidor
        """
        with self.servers_lock:
            for i, s in enumerate(self.servers):
                if s.ip == server_ip and s.port == server_port:
                    self.servers[i].active = False
                    break