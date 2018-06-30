""" Simple python Kademlia DHT data store implementation.
Useful for distributing a key-value store in a decentralized manner.

To create a new DHT swarm, just call DHT() with the host and port that you will listen on. To join an existing DHT swarm, also provide bootstrap host and port numbers of any existing node.  The nodes will discover the rest of the swarm as appropriate during usage.
"""

import json
import logging
import random
import socket
import socketserver
import sys
import threading
import time
from .bucketset import BucketSet
from .hashing import hash_function, random_id
from .peer import Peer
from .storage import Shelve
from .shortlist import Shortlist
from . import hashing

# hack to handle long integers in both python2 & 3 (where just have int)
if sys.version_info > (3,):
	long = int

k = 20
alpha = 3
id_bits = 128
iteration_sleep = 1

class DHTRequestHandler(socketserver.BaseRequestHandler):
	def handle(self):
		try:
			message = json.loads(self.request[0].decode('utf-8').strip())
			self.server.dht.logger.debug('handle request(%s)' % (str(message)))
			message_type = message["message_type"]
			if message_type == "ping":
				self.handle_ping(message)
			elif message_type == "pong":
				self.handle_pong(message)
			elif message_type == "find_node":
				self.handle_find(message)
			elif message_type == "find_value":
				self.handle_find(message, find_value=True)
			elif message_type == "found_nodes":
				self.handle_found_nodes(message)
			elif message_type == "found_value":
				self.handle_found_value(message)
			elif message_type == "store":
				self.handle_store(message)
		except KeyError:
			pass
		except ValueError:
			pass
		client_host, client_port = self.client_address
		peer_id = message["peer_id"]
		peer_info = message["peer_info"]
		new_peer = Peer(client_host, client_port, peer_id, peer_info)
		self.server.dht.buckets.insert(new_peer)

	def handle_ping(self, message):
		client_host, client_port = self.client_address
		id = message["peer_id"]
		info = message["peer_info"]
		peer = Peer(client_host, client_port, id, info)
		peer.pong(socket=self.server.socket, peer_id=self.server.dht.peer.id, lock=self.server.send_lock)

	def handle_pong(self, message):
		pass

	def handle_find(self, message, find_value=False):
		key = message["id"]
		id = message["peer_id"]
		info = message["peer_info"]
		client_host, client_port = self.client_address
		peer = Peer(client_host, client_port, id, info)
		response_socket = self.request[1]
		self.server.dht.data_lock.acquire()
		if find_value and (str(key) in self.server.dht.data):
			value = self.server.dht.data[str(key)]
			self.server.dht.data_lock.release()
			peer.found_value(id, value, message["rpc_id"], socket=response_socket, peer_id=self.server.dht.peer.id, peer_info=self.server.dht.peer.info, lock=self.server.send_lock)
		else:
			self.server.dht.data_lock.release()
			nearest_nodes = self.server.dht.buckets.nearest_nodes(id)
			if not nearest_nodes:
				nearest_nodes.append(self.server.dht.peer)
			nearest_nodes = [nearest_peer.astriple() for nearest_peer in nearest_nodes]
			peer.found_nodes(id, nearest_nodes, message["rpc_id"], socket=response_socket, peer_id=self.server.dht.peer.id, peer_info=self.server.dht.peer.info, lock=self.server.send_lock)

	def handle_found_nodes(self, message):
		rpc_id = message["rpc_id"]
		with self.server.dht.rpc_ids_lock:
			shortlist = self.server.dht.rpc_ids[rpc_id]
			del self.server.dht.rpc_ids[rpc_id]
		nearest_nodes = [Peer(*peer) for peer in message["nearest_nodes"]]
		shortlist.update(nearest_nodes)

	def handle_found_value(self, message):
		rpc_id = message["rpc_id"]
		with self.server.dht.rpc_ids_lock:
			shortlist = self.server.dht.rpc_ids[rpc_id]
			del self.server.dht.rpc_ids[rpc_id]
		shortlist.set_complete(message["value"])

	def handle_store(self, message):
		key = message["id"]
		value = message["value"]
		self.server.dht.logger.info('store ("%s",%s)' % (str(key),str(value)))
		with self.server.dht.data_lock:
			self.server.dht.data[str(key)] = value


class DHTServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
	""" UDP Server for a DHT instance listening on host:port.

	Uses the specified handler class (default is DHTRequestHandler).
	A reference to the DHT instance is saved in self.dht after creation
	by DHT __init__ to allow the handler class to access the DHT data store.
	"""
	def __init__(self, host_address, handler_cls):
		socketserver.UDPServer.__init__(self, host_address, handler_cls)
		self.send_lock = threading.Lock()

	def server_close(self):
		self.dht.logger.debug('server closing down')
		return socketserver.UDPServer.server_close(self)

class DHT(object):
	""" DHT node instance with associated server listening on host:port,
	with a new random id (unless explicitly specified).

	The server will connect to other node(s) listed in seeds to
	bootstrap into an existing DHT swarm. If no seeds given, then
	presumably are starting a new swarm.

	Uses the specified storage (eg shelve or pysos, defaults to dict).
	Can provide some option info details, distributed with requests.
	The hashing function and requesthandler can also be over-ridden.
	"""
	def __init__(self, host, port, id=None, seeds=[], storage={}, info={}, hash_function=hashing.hash_function, requesthandler=DHTRequestHandler):
		if not id:
			id = random_id()
		self.storage = storage
		self.info = info
		self.hash_function = hash_function
		self.peer = Peer(host, port, id, info)
		self.data = self.storage
		self.data_lock = threading.Lock()
		self.buckets = BucketSet(k, id_bits, self.peer.id)
		self.rpc_ids = {}
		self.rpc_ids_lock = threading.Lock()
		self.server = DHTServer(self.peer.address(), requesthandler)
		self.server.dht = self
		self.logger = logging.getLogger('DHT(%s:%d)' % (self.peer.host, self.peer.port))
		self.logger.info('server init(id="%s")' % (id))
		self.server_thread = threading.Thread(target=self.server.serve_forever)
		self.server_thread.daemon = True
		self.server_thread.start()
		self.bootstrap(seeds)
		self.logger.info('peers="%s"' % (str(self.peers())))

	def identity(self):
		return self.peer.id


	def iterative_find_nodes(self, key, boot_peer=None):
		shortlist = Shortlist(k, key)
		shortlist.update(self.buckets.nearest_nodes(key, limit=alpha))
		if boot_peer:
			rpc_id = random.getrandbits(id_bits)
			with self.rpc_ids_lock:
				self.rpc_ids[rpc_id] = shortlist
			boot_peer.find_node(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id, peer_info=self.peer.info)
		while (not shortlist.complete()) or boot_peer:
			nearest_nodes = shortlist.get_next_iteration(alpha)
			for peer in nearest_nodes:
				shortlist.mark(peer)
				rpc_id = random.getrandbits(id_bits)
				with self.rpc_ids_lock:
					self.rpc_ids[rpc_id] = shortlist
				peer.find_node(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id, peer_info=self.info) ######
			time.sleep(iteration_sleep)
			boot_peer = None
		return shortlist.results()

	def iterative_find_value(self, key):
		shortlist = Shortlist(k, key)
		shortlist.update(self.buckets.nearest_nodes(key, limit=alpha))
		while not shortlist.complete():
			nearest_nodes = shortlist.get_next_iteration(alpha)
			for peer in nearest_nodes:
				shortlist.mark(peer)
				rpc_id = random.getrandbits(id_bits)
				with self.rpc_ids_lock:
					self.rpc_ids[rpc_id] = shortlist
				peer.find_value(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id, peer_info=self.info) #####
			time.sleep(iteration_sleep)
		return shortlist.completion_result()



	# Return the list of connected peers
	def peers(self):
		return self.buckets.to_dict()

	# Boostrap the network with a list of bootstrap nodes
	def bootstrap(self, bootstrap_nodes = []):
		for bnode in bootstrap_nodes:
			boot_peer = Peer(bnode[0], bnode[1], "", "")
			self.iterative_find_nodes(self.peer.id, boot_peer=boot_peer)

		if len(bootstrap_nodes) == 0:
			for bnode in self.buckets.to_list():
				self.iterative_find_nodes(self.peer.id, boot_peer=Peer(bnode[0], bnode[1], bnode[2], bnode[3]))



	# Get a value in a sync way, calling an handler
	def get_sync(self, key, handler):
		try:
			d = self[key]
		except:
			d = None

		handler(d)


	# Get a value in async way
	def get(self, key, handler):
		t = threading.Thread(target=self.get_sync, args=(key, handler))
		t.start()


	# Iterator
	def __iter__(self):
		with self.data_lock:
			data_iter = iter(map(lambda key: long(key), self.data.__iter__()))
		return data_iter

	# Operator []
	def __getitem__(self, key):
		if type(key) == long:
			hashed_key = key
		else:
			hashed_key = self.hash_function(key)
		self.data_lock.acquire()
		if str(hashed_key) in self.data:
			result = self.data[str(hashed_key)]
			self.data_lock.release()
			self.logger.debug('get("%s") at %d is %s' % (key, hashed_key, str(result)))
			return result
		else:	
			self.data_lock.release()
		result = self.iterative_find_value(hashed_key)
		if result:
			self.logger.debug('get("%s") at %d is %s' % (key, hashed_key, str(result)))
			return result
		raise KeyError

	# Operator []=
	def __setitem__(self, key, value):
		hashed_key = self.hash_function(key)
		self.logger.info('set("%s",%s) at %d' % (key, str(value), hashed_key))
		nearest_nodes = self.iterative_find_nodes(hashed_key)
		if not nearest_nodes:
			with self.data_lock:
				self.data[str(hashed_key)] = value
		for node in nearest_nodes:
			node.store(hashed_key, value, socket=self.server.socket, peer_id=self.peer.id)

	def tick():
		pass
