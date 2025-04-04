import json
import socket
import struct
import time
from typing import Tuple, Dict

try:
    from .server_manager import ServerManager
    from .distributed_sort import DistributedSorter
    from .utils import logger
except ImportError:
    from server_manager import ServerManager
    from distributed_sort import DistributedSorter
    from utils import logger

class ClientHandler:
    """Maneja las conexiones y solicitudes de los clientes."""
    
    def __init__(self, server_manager: ServerManager, distributed_sorter: DistributedSorter):

        self.server_manager = server_manager
        self.distributed_sorter = distributed_sorter
        
        # Stats de operaciones
        self.total_operations = 0
        self.successful_operations = 0
    
    def handle_list(self, client_socket: socket.socket) -> bool:

        self.server_manager.update_servers_status()
        active_servers = self.server_manager.get_active_servers()
        
        if not active_servers:
            mensaje = "\n❌ ERROR: No hay servidores disponibles para el procesamiento.\n"
            client_socket.send(mensaje.encode())
            return False

        mensaje = "\n════════════════════════════════════════════\n"
        mensaje += "🚀 LAB 5 - SISTEMAS DIST \n"
        mensaje += "════════════════════════════════════════════\n\n"
        mensaje += f"Servidores disponibles: {len(active_servers)}\n"
        
        # info de los servidores
        for i, server in enumerate(active_servers, 1):
            tiempo_resp = f"{server.last_response_time:.2f}s" if server.last_response_time > 0 else "N/A"
            mensaje += f"{i}. {server.name} ({server.ip}:{server.port}) - Tiempo resp: {tiempo_resp}\n"
        
        mensaje += "Iniciando procesamiento distribuido...\n"
        
        client_socket.send(mensaje.encode())
        return True
    
    def handle_distributed_sort(self, client_socket: socket.socket) -> None:
        """
        Maneja una solicitud de ordenamiento distribuido.
        
        Args:
            client_socket: Socket del cliente
        """
        try:
            # checar servidores disponibles
            active_count, total_count = self.server_manager.update_servers_status()
            
            if active_count == 0:
                response = {
                    "error": "No hay servidores disponibles para el procesamiento distribuido",
                    "mensaje": "Intente nuevamente más tarde cuando haya servidores conectados"
                }
                client_socket.send(json.dumps(response).encode())
                return
            
            response = {
                "status": "ready", 
                "mensaje": "Ordenamiento distribuido listo pa procesar"
            }
            client_socket.send(json.dumps(response).encode())
            
            # Recibir datos del cliente (primero la longitud)
            try:
                raw_msglen = client_socket.recv(4)
                if not raw_msglen:
                    logger.warning("Cliente cerró la conexión sin enviar datos")
                    return
                    
                msglen = struct.unpack('!I', raw_msglen)[0]
                logger.info(f"Recibiendo {msglen/(1024*1024):.2f} MB de datos del cliente")
                
                # Recibir todos los datos del cliente
                data = b""
                bytes_received = 0
                client_socket.settimeout(300)  # 5 minutos por si son datod grandes
                
                # Usar un buffer de recepción
                while bytes_received < msglen:
                    to_read = min(msglen - bytes_received, 1024 * 1024)
                    chunk = client_socket.recv(to_read)
                    
                    if not chunk:
                        logger.warning(f"Cliente cerró conexión después de {bytes_received}/{msglen} bytes")
                        return
                        
                    data += chunk
                    bytes_received += len(chunk)
                    
                    #  progreso cada 10MB
                    if bytes_received % (10 * 1024 * 1024) == 0:
                        logger.info(f"Recibido: {bytes_received/(1024*1024):.1f} MB / {msglen/(1024*1024):.1f} MB")
                
                logger.info(f"✅ Datos recibidos: {bytes_received} bytes")
                
                # Actualizar stats
                self.distributed_sorter.stats["bytes_processed"] += bytes_received
                
                try:
                    numbers = json.loads(data.decode())
                    if not isinstance(numbers, list):
                        raise ValueError("Los datos recibidos no son una lista")
                        
                    logger.info(f"Recibidos {len(numbers)} números para ordenar")
                    
                    # Proceso de ordenamiento distribuido
                    success = self.distributed_sorter.process_distributed(numbers, client_socket)
                    
                    # Actualizar stats
                    self.total_operations += 1
                    if success:
                        self.successful_operations += 1
                    
                except json.JSONDecodeError:
                    response = {"error": "Los datos enviados no tienen formato JSON válido"}
                    client_socket.send(json.dumps(response).encode())
                
            except struct.error:
                logger.error("Error al decodificar longitud del mensaje")
                response = {"error": "Error de protocolo: formato de mensaje inválido"}
                client_socket.send(json.dumps(response).encode())
                
        except Exception as e:
            logger.error(f"Error en procesamiento distribuido: {e}")
            self.distributed_sorter.stats["total_errors"] += 1
            try:
                response = {"error": str(e)}
                client_socket.send(json.dumps(response).encode())
            except:
                pass
    
    def handle_client(self, client_socket: socket.socket, address: Tuple[str, int]) -> None:

        client_id = f"{address[0]}:{address[1]}"
        
        try:
            logger.info(f"Cliente {client_id} conectado")
            data = client_socket.recv(8192).decode()
            
            if data == "LIST":
                logger.info(f"Cliente {client_id} solicitó lista de servidores")
                if self.handle_list(client_socket):
                    self.handle_distributed_sort(client_socket)
                return
            
            try:
                json_data = json.loads(data)
                action = json_data.get('action')

                if action == "register":
                    response = self.server_manager.register_server(json_data)
                    client_socket.send(json.dumps(response).encode())
                elif action == "sort":
                    logger.info(f"Cliente {client_id} pidió ordenamiento")
                    self.handle_distributed_sort(client_socket)
                else:
                    response = {"status": "error", "mensaje": f"Acción desconocida: {action}"}
                    client_socket.send(json.dumps(response).encode())
                    logger.warning(f"Cliente {client_id} envió acción desconocida: {action}")

            except json.JSONDecodeError:
                logger.warning(f"Cliente {client_id} envió datos con formato inválido")
                response = {"status": "error", "mensaje": "Formato JSON inválido"}
                client_socket.send(json.dumps(response).encode())

        except ConnectionResetError:
            logger.warning(f"Cliente {client_id} desconectado abruptamente")
        except Exception as e:
            logger.error(f"Error manejando cliente {client_id}: {e}")
        finally:
            client_socket.close()
            logger.info(f"Conexión con cliente {client_id} cerrada")