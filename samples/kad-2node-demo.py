""" KAD example two-node DHT, with storage, and iteration.

Usage: kad-2node-demo.py [-h] [-v]

Creates 2 linked KAD DHT nodes, assigns & retreieves some values,
then iterates over all values known to 1 of the nodes.

Uses "pysos.py" module for storage so creates plain text files compatible 
when run with either python v2 & v3.

Code adapted from project github.com/dakk/kad.py - README.md by
Lawrie Brown June 2018.
"""

from __future__ import print_function	# needed so print usable in lambda
import argparse
from kad import DHT			# using KAD DHT class
import logging
import sys

try:			# provide persistent storage for nodes if available
	import pysos
	have_storage = True
except:
	have_storage = False

# parse command-line args (except when generating pydoc)
parser = argparse.ArgumentParser(description='Demo 2node KAD DHT servers.')
parser.add_argument('-v', '--verbose', help='enable (increasingly verbose) debug diagnostics', action='count')
if sys.argv[0].find('pydoc') > 0:
	sys.argv = [sys.argv[0]]
args = parser.parse_args()

# set loglevel to use (DEBUG/INFO/WARNING) and message format
loglevel = logging.INFO
if args.verbose != None and args.verbose > 0:	loglevel = logging.DEBUG
logging.basicConfig(level=loglevel, format='%(asctime)s %(name)s %(levelname)s: %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

# create 2 DHT server nodes
host1, port1 = 'localhost', 14900
if have_storage:	store1 = pysos.Dict('sto%d.dat' % port1)
else:			store1 = {}
dht1 = DHT(host1, port1, storage=store1)
myseeds = [(host1, port1)]
host2, port2 = 'localhost', 14901
if have_storage:	store2 = pysos.Dict('sto%d.dat' % port2)
else:			store2 = {}
dht2 = DHT(host2, port2, seeds=myseeds, storage=store2)

# store some values via DHT1 or 2
dht1["my_key"] = [u"My", u"json-serializable", u"Object"]
dht1["akey1"] = ["Some","Strings","for key1"]
dht1["akey2"] = ["Other","Strings","for key2"]
dht2["akey3"] = ["Further","Strings","for key3"]

# demo retrieve data from nodes
print ("Blocking get from DHT1 = %s" % str(dht2["my_key"]))
dht2.get ("my_key", lambda data: print("Threaded get from DHT2 = %s" % str(data)) )

# test iteration over node data (only see values known on the node)
print("Iterating over all values as seen by DHT2:")
for key in dht2:
    print('"%s" -> %s' % (key, dht2[key]))

# shutdown servers
dht1.server.shutdown()
dht1.server.server_close()
dht2.server.shutdown()
dht2.server.server_close()

