#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import grpc

import csci4220_hw4_pb2
import csci4220_hw4_pb2_grpc

local_id = 0
my_address = ''
my_hostname = ''
my_port = 0
k_buckets = []
k = 0

# <key,value> pairs
pairs = dict()

bits = 0

# Build the Program according to the protocol
class Program(csci4220_hw4_pb2_grpc.KadImplServicer):

	# implement the methods in the protocol
	def FindNode(self, call, setting):
		print('Serving FindNode({}) request for {}'.format(call.idkey, call.node.id))

		S = get_k_closest(call.idkey)
		# update k_buckets
		update(call.node)

		# return k closest nodes to this nodeID
		return csci4220_hw4_pb2.NodeList(responding_node=myself(), nodes=S)

	def FindValue(self, call, setting):
		"""Complicated - we might get a value back, or we might get k nodes with
		distance closest to key called
		"""
		print('Serving FindKey({}) request for {}'.format(call.idkey, call.node.id))

		S = get_k_closest(call.idkey)
		# update k-buckets
		update(call.node)
		# return value if its in local
		if call.idkey in pairs:
			return csci4220_hw4_pb2.KV_Node_Wrapper(responding_node=myself(), mode_kv=True, 
				kv=csci4220_hw4_pb2.KeyValue(key=call.idkey, value=pairs[call.idkey]))
		
		# return k closest nodes
		return  csci4220_hw4_pb2.KV_Node_Wrapper(responding_node=myself(), mode_kv=False, nodes=S)

	def Store(self, call, setting):
		# store this pair
		pairs[call.key] = call.value
		print('Storing key {} value "{}"'.format(call.key, call.value))
		return csci4220_hw4_pb2.IDKey(node=myself(), idkey=local_id)

	def Quit(self, call, setting):
		for i in range(bits):
			for j in range(len(k_buckets[i])):
				if k_buckets[i][j].id == call.idkey:
					# remove the quiting node from k_buckets
					print('Evicting quitting node {} from bucket {}'.format(call.idkey, i))
					k_buckets[i].remove(k_buckets[i][j])
					return call
		
		print('No record of quitting node {} in k-buckets.'.format(call.idkey))
		return call

# return the node itself
def myself():
	return csci4220_hw4_pb2.Node(id=local_id, address=my_address, port=my_port)

# print k_buckets
def print_k_bucket():
	for i in range(bits):
		print('{}:'.format(i), end = '')
		for j in range(len(k_buckets[i])):
			print(' {}:{}'. format(k_buckets[i][j].id, k_buckets[i][j].port), end = '')
		print()

	return

# update k_buckets according to the responding node
def update(responding_node):
	if responding_node.id == local_id:
		return

	# calculate the index in k_buckets
	distance = responding_node.id ^ local_id
	index = 0
	for i in range(bits):
		if 2**i >= distance:
			index = i
			break

	# if the responding node already exists in k_buckets, no need to add
	for refresh in range(len(k_buckets[index])):
		if responding_node.id == k_buckets[index][refresh].id:
			return

	if len(k_buckets[index]) != k:
		k_buckets[index].append(responding_node)
		return
	else:
		del k_buckets[index][0]
		k_buckets[index].append(responding_node)
		return

# get k closest nodes based on this nodeID
def get_k_closest(nodeID):
	allNodes = []
	k_closest = []
	for i in range(bits):
		for j in range(len(k_buckets[i])):
			allNodes.append(k_buckets[i][j])
	
	# sort the nodes by XOR distance
	allNodes.sort(key=lambda node: node.id ^ nodeID)

	var = 0
	# if the number of nodes is less than k, then return all
	if len(allNodes) < k:
		var = len(allNodes)
	else:
		var = k
	for i in range(var):
		k_closest.append(allNodes[i])

	return k_closest

# BOOTSTRAP <remote hostname> <remote port>
def bootStrap(remote_hostname, remote_port):
	# build the connection between two nodes
	channel = grpc.insecure_channel(remote_hostname + ":" + remote_port)
	stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
	response = stub.FindNode(csci4220_hw4_pb2.IDKey(idkey=local_id, node=myself()))

	# add nodes on each other's k_buckets
	update(response.responding_node)
	for node in response.nodes:
		update(node)

	# print k_buckets
	print("After BOOTSTRAP({}), k_buckets now look like:".format(response.responding_node.id))
	print_k_bucket()
	return

# FIND_NODE <nodeID>
def findNode(nodeID):
	if local_id == nodeID:
		print('After FIND_NODE command, k-buckets are:')
		print_k_bucket()
		print('Found destination id {}'.format(nodeID))
		return

	S = get_k_closest(nodeID)
	S_ = S.copy()

	foundNode = False
	for node in S:
		if foundNode:
			break

		# build the connection between two nodes
		channel = grpc.insecure_channel(node.address + ":" + str(node.port))
		stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
		R = stub.FindNode(csci4220_hw4_pb2.IDKey(idkey=nodeID, node=myself()))
		if R.responding_node.id == nodeID:
			print('Found destination id {}'.format(nodeID))
			foundNode = True
			break

		for r in R:
			update(r)
			if r.id == nodeID:
				print('Found destination id {}'.format(nodeID))
				foundNode = True
				break

		# remove the visited node
		S_.remove(node)
		
	if not foundNode:
		print('Could not find destination id {}'.format(nodeID))

	print('After FIND_NODE command, k-buckets are:')
	print_k_bucket()

#FIND_VALUE <key>
def findValue(key):
	# if key is at local
	if key in pairs:
		print('Found data "{}" for key {}'.format(pairs[key], key))
		print('After FIND_VALUE command, k-buckets are:')
		print_k_bucket()
		return

	S = get_k_closest(key)
	S_ = S.copy()

	foundNode = False

	for node in S:
		# build the connection between two nodes
		channel = grpc.insecure_channel(node.address + ":" + str(node.port))
		stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
		R = stub.FindValue(csci4220_hw4_pb2.IDKey(idkey=key, node=myself()))

		for r in R.nodes:
			update(r)
		update(R.responding_node)

		# FOUND
		if R.mode_kv:
			foundNode = True
			print('Found value "{}" for key {}'.format(R.kv.value, key))
			break

		#remove the visited node
		S_.remove(node)
		
	if not foundNode:
		print('Could not find key {}'.format(key))

	print('After FIND_VALUE command, k-buckets are:')
	print_k_bucket()

# STORE <key> <value>
def store(key, value):
	S = get_k_closest(key)
	# if need to store pairs locally
	if len(S) == 0:
		pairs[key] = value
		print('Storing key {} at node {}'.format(key, local_id))
		return

	# if need to store pairs locally
	if local_id ^ key < S[0].id ^ key:
		pairs[key] = value
		print('Storing key {} at node {}'.format(key, local_id))
		return

	else:
		# save to closest node
		# build the connection between two nodes
		channel = grpc.insecure_channel(S[0].address + ":" + str(S[0].port))
		stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
		stub.Store(csci4220_hw4_pb2.KeyValue(node=myself(), key=key, value=value))
		print('Storing key {} at node {}'.format(key, S[0].id))
		return

# QUIT command
def quit():
	# collect all nodes in k_buckets
	allNodes = []
	for i in range(bits):
		for j in range(len(k_buckets[i])):
			allNodes.append(k_buckets[i][j])

	# quit each node
	for n in allNodes:
		print('Letting {} know I\'m quitting.'.format(n.id))
		channel = grpc.insecure_channel(n.address + ":" + str(n.port))
		stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
		stub.Quit(csci4220_hw4_pb2.IDKey(idkey=local_id))

def run():
	if len(sys.argv) != 4:
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)


	global local_id
	global k_buckets
	global k
	global my_address
	global my_port
	global my_hostname
	global bits

	local_id = int(sys.argv[1])
	my_port = int(sys.argv[2]) # add_insecure_port() will want a string
	k = int(sys.argv[3])
	my_hostname = socket.gethostname() # Gets my host name
	my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname

	# calculate the bits and initialize k_buckets
	temp = my_port
	while temp != 0:
		bits += 1
		temp = temp // 10

	for i in range(bits):
		k_buckets.append([])

	program = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	csci4220_hw4_pb2_grpc.add_KadImplServicer_to_server(Program(), program)
	program.add_insecure_port('[::]:' + str(my_port))
	program.start()
	
	# process commands 
	while True:
		typein = input('')
		command = typein.split()
		# BOOTSTRAP
		if command[0] == 'BOOTSTRAP':
			bootStrap(command[1], command[2])

		# FIND_NODE
		elif command[0] == 'FIND_NODE':
			print('Before FIND_NODE command, k-buckets are:')
			print_k_bucket()

			findNode(int(command[1]))

		# FIND_VALUE
		elif command[0] == 'FIND_VALUE':
			print('Before FIND_VALUE command, k-buckets are:')
			print_k_bucket()

			findValue(int(command[1]))

		# STORE
		elif command[0] == 'STORE':
			store(int(command[1]), command[2])
		
		# QUIT
		elif command[0] == 'QUIT':
			quit()
			print('Shut down node {}'.format(local_id))
			program.stop(0)
			break

if __name__ == '__main__':
	run()