import heapq
import json
import socket
import struct
import threading
import time
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

try:
    from .server_manager import ServerInfo
    from .utils import logger
except ImportError:
    from server_manager import ServerInfo
    from utils import logger

@dataclass
class ServerResult:
    """Resultados de procesamiento de un servidor."""
    server: ServerInfo
    data: List[int]
    success: bool = True
    error_message: str = ""
    processing_time: float = 0.0


class DistributedSorter:
    
    def __init__(self, server_manager):

        self.server_manager = server_manager
        # Stats de operaciones
        self.stats = {
            "bytes_processed": 0,
            "numbers_sorted": 0,
            "total_errors": 0,
            "avg_response_time": 0.0
        }
    
    def merge_sorted_lists(self, lists: List[List[int]]) -> List[int]:

        if not lists:
            return []
        if len(lists) == 1:
            return lists[0]
            
        result = []
        heap = []
        
        for i, lst in enumerate(lists):
            if lst:  
                heapq.heappush(heap, (lst[0], 0, i))
        
        while heap:
            val, idx, list_idx = heapq.heappop(heap)
            result.append(val)
            
            if idx + 1 < len(lists[list_idx]):
                heapq.heappush(heap, (lists[list_idx][idx + 1], idx + 1, list_idx))
        
        return result
    
    def send_chunk_to_server(self, server: ServerInfo, chunk: List[int]) -> ServerResult:
        """
        Manda un lote de datos a un servidor y recibe el resultado ordenado.
        
        Args:
            server: Información del servidor
            chunk: Lista de nums a ordenar
            

        """
        try:
            logger.info(f"Enviando {len(chunk)} números al servidor {server.name}")
            
            # Medir tiempo de procesamiento
            start_time = time.time()
            
            # Conectar al servidor con timeout
            with socket.create_connection((server.ip, server.port), timeout=10) as server_sock:
                server_sock.settimeout(300)  
                json_data = json.dumps(chunk).encode()
                
                # Enviar prefijo con longitud
                server_sock.sendall(struct.pack('!I', len(json_data)))
                
                # Enviar datos en bloques
                chunk_size = 1024 * 1024  # 1MB por chunk
                for i in range(0, len(json_data), chunk_size):
                    end_idx = min(i + chunk_size, len(json_data))
                    server_sock.sendall(json_data[i:end_idx])
                
                # Recibir respuesta del servidor (primero la longitud)
                raw_msglen = server_sock.recv(4)
                if not raw_msglen:
                    raise ConnectionError(f"Servidor {server.name} cerró la conexión sin responder")
                    
                msglen = struct.unpack('!I', raw_msglen)[0]
                logger.info(f"Recibiendo respuesta de {msglen/(1024*1024):.2f} MB del servidor {server.name}")
                
                # Recibir datos completos
                data = b""
                bytes_received = 0
                
                while bytes_received < msglen:
                    to_read = min(msglen - bytes_received, 1024 * 1024)
                    chunk_data = server_sock.recv(to_read)
                    
                    if not chunk_data:
                        raise ConnectionError(f"Servidor {server.name} cerró la conexión durante la transferencia")
                        
                    data += chunk_data
                    bytes_received += len(chunk_data)
                
                processing_time = time.time() - start_time
                
                # Actualizar stats del servidor
                self.server_manager.update_server_stats(
                    server.ip, server.port, processing_time, len(chunk)
                )
                
                # Decodificar y validar resultado
                try:
                    sorted_chunk = json.loads(data.decode())
                    if not isinstance(sorted_chunk, list):
                        raise ValueError("El servidor no devolvió una lista")
                        
                    logger.info(f"Servidor {server.name} respondió con {len(sorted_chunk)} números ordenados en {processing_time:.2f}s")
                    return ServerResult(
                        server=server, 
                        data=sorted_chunk, 
                        success=True,
                        processing_time=processing_time
                    )
                except json.JSONDecodeError:
                    raise ValueError(f"El servidor {server.name} devolvió datos con formato inválido")
                
        except Exception as e:
            logger.error(f"Error con servidor {server.name}: {e}")
            
            # Update datos del servidor
            self.server_manager.mark_server_inactive(server.ip, server.port)
            
            processing_time = time.time() - start_time if 'start_time' in locals() else 0
            return ServerResult(
                server=server, 
                data=[], 
                success=False, 
                error_message=str(e),
                processing_time=processing_time
            )
    
    def process_distributed(self, numbers: List[int], client_socket: socket.socket) -> bool:
        """
        Procesa una lista de nums utilizando ordenamiento distribuido.
        
        Args:
            numbers: Lista de números a ordenar
            client_socket: Socket del cliente para enviar respuesta
            
        Returns:
            bool: True si el procesamiento fue exitoso, False en caso contrario
        """
        try:
            # Verificar servidores disponibles
            self.server_manager.update_servers_status()
            active_servers = self.server_manager.get_active_servers()
            
            if not active_servers:
                response = {"error": "No hay servidores disponibles para procesar"}
                client_socket.send(json.dumps(response).encode())
                return False
            
            # Actualizar estadísticas
            self.stats["numbers_sorted"] += len(numbers)
            
            # Distribuir los nums entre los servidores
            server_count = len(active_servers)
            chunk_size = max(1, len(numbers) // server_count)
            chunks = []
            
            # Crear chunks 
            for i in range(server_count):
                start_idx = i * chunk_size
                end_idx = start_idx + chunk_size if i < server_count - 1 else len(numbers)
                chunks.append(numbers[start_idx:end_idx])
            
            logger.info(f"Distribuyendo datos en {server_count} servidores")
            
            # Enviar chunks a los servidores en paralelo
            threads = []
            results = [None] * server_count
            
            def worker(idx, server, chunk):
                """Función worker que procesa un chunk y guarda el resultado"""
                result = self.send_chunk_to_server(server, chunk)
                results[idx] = result
            
            # Iniciar hilos para procesamiento paralelo
            for i, (server, chunk) in enumerate(zip(active_servers, chunks)):
                thread = threading.Thread(target=worker, args=(i, server, chunk))
                thread.daemon = True
                threads.append(thread)
                thread.start()
            
            # Esperar a que todos los hilos terminen
            for thread in threads:
                thread.join()
            
            # Procesar resultados
            successful_results = []
            failed_servers = []
            
            for result in results:
                if result and result.success:
                    successful_results.append(result.data)
                else:
                    if result:
                        failed_servers.append(result.server.name)
                        self.stats["total_errors"] += 1
            
            if not successful_results:
                response = {"error": f"Todos los servidores fallaron: {', '.join(failed_servers)}"}
                client_socket.send(json.dumps(response).encode())
                return False
            
            # errores parciales
            if failed_servers:
                logger.warning(f"⚠️ Algunos servidores fallaron: {', '.join(failed_servers)}")
            
            # Combinar resultados 
            logger.info("Combinando resultados ordenados...")
            start_merge = time.time()
            final_sorted = self.merge_sorted_lists(successful_results)
            logger.info(f"Combinación completada en {time.time() - start_merge:.2f}s")
            
            # Enviar resultado al cliente
            logger.info(f"Enviando resultado final ({len(final_sorted)} números)")
            json_result = json.dumps(final_sorted).encode()
            
            # Enviar longitud primero
            client_socket.sendall(struct.pack('!I', len(json_result)))
            
            # Enviar datos en bloques
            chunk_size = 1024 * 1024  # 1MB por chunk
            bytes_sent = 0
            
            for i in range(0, len(json_result), chunk_size):
                end_idx = min(i + chunk_size, len(json_result))
                client_socket.sendall(json_result[i:end_idx])
                bytes_sent += end_idx - i
                
                # Mostrar progreso cada 10MB
                if bytes_sent % (10 * 1024 * 1024) == 0:
                    logger.info(f"Enviado: {bytes_sent/(1024*1024):.1f} MB / {len(json_result)/(1024*1024):.1f} MB")
            
            logger.info(f"✅ Resultado enviado: {bytes_sent} bytes")
            return True
            
        except Exception as e:
            logger.error(f"Error en procesamiento distribuido: {e}")
            self.stats["total_errors"] += 1
            try:
                response = {"error": str(e)}
                client_socket.send(json.dumps(response).encode())
            except:
                pass
            return False