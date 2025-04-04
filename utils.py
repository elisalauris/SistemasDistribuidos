
import socket
import logging
import sys

# Config del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger = logging.getLogger('middleware-distribuido')

def get_local_ip() -> str:
    """
    Obtiene la dirección IP en la red local
    
    Returns:
        str: Dirección IP local o 127.0.0.1 en caso de error
    """
    try:
        # Crear un socket y conectar a un servidor externo para determinar la interfaz
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception as e:
        logger.warning(f"No se pudo determinar la IP local: {e}")
        return "127.0.0.1"