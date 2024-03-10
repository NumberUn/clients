import aiohttp
import socket


class CustomTCPConnector(aiohttp.TCPConnector):
    async def _create_connection(self, req, traces, timeout):
        conn = await super()._create_connection(req, traces, timeout)
        if conn.transport and conn.transport.get_extra_info('socket'):
            # Set TCP_NODELAY to True directly on the socket
            sock = conn.transport.get_extra_info('socket')
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return conn