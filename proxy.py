import argparse
import json
import logging
import sys

from gevent.server import DatagramServer
from gevent.pool import Pool
from gevent.event import Event
import gevent.socket as socket


logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


def read_map(fp):
    if not fp:
        fp = file('./portmap.json')
        logger.info('No config file specified, using default portmap.json.')
    try:
        portmap = json.load(fp)
    except IOError:
        logger.error('No config file can be loaded, exiting...')
        exit(1)
    except AttributeError as ae:
        logger.exception(ae)

    return portmap


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='The configuration file, default ./portmap.json')
    parser.add_argument('-l', '--log',
                        help='Log file location.')
    return parser.parse_args()


class UDPProxy(DatagramServer):
    def __init__(self, local_port, dst_addr):
        """
        portmap is in format as: 623: ['10.128.20.41', 623]
        """
        pool = Pool(10)
        super(UDPProxy, self).__init__(local_port, spawn=pool)
        self.reuse_addr = None
        self.dst_addr = dst_addr

    def handle(self, data, addr):
        sock_dst = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if not data:
            logger.error('an error occured')
            return
        logger.debug('received: {0!r} from {1}.'.format(data, addr))
        sock_dst.sendto(data, self.dst_addr)
        logger.debug('send: {0!r} to {1}.'.format(data, self.dst_addr))
        data, _ = sock_dst.recvfrom(65565)
        self.socket.sendto(data, addr)


# https://gist.github.com/denik/1008826
class ServerPack(object):
    def __init__(self, servers):
        self.servers = servers
        self._stop_event = Event()
        self._stop_event.set()

    def start(self):
        self._stop_event.clear()
        started = []
        try:
            for server in self.servers[:]:
                server.start()
                started.append(server)
                name = getattr(server, 'name', None) or server.__class__.__name__ or 'Server'
                logger.info('%s started on %s', name, server.address)
        except:
            self.stop(started)
            raise
        self._stop_event.wait()

    def stop(self, servers=None):
        self._stop_event.set()
        if servers is None:
            servers = self.servers[:]
        for server in servers:
            try:
                server.stop()
            except:
                if hasattr(server, 'loop'):  # gevent >= 1.0
                    server.loop.handle_error(server.stop, *sys.exc_info())
                else:  # gevent <= 0.13
                    import traceback
                    traceback.print_exc()


if __name__ == '__main__':
    args = parse_args()
    servers = []
    portmap = read_map(args.config)
    for key in portmap:
        server = UDPProxy(key, tuple(portmap[key]))
        servers.append(server)

    sp = ServerPack(servers)
    try:
        sp.start()
    except KeyboardInterrupt:
        sp.stop()
