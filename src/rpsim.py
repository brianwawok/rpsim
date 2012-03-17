#Copyright (c) 2012, Brian Wawok (bwawok@gmail.com)
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#	* Redistributions of source code must retain the above copyright
#	  notice, this list of conditions and the following disclaimer.
#	* Redistributions in binary form must reproduce the above copyright
#	  notice, this list of conditions and the following disclaimer in the
#	  documentation and/or other materials provided with the distribution.
#	* Neither the name of the <organization> nor the
#	  names of its contributors may be used to endorse or promote products
#	  derived from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.



import os, string, cPickle, time, math
import asyncore, socket
import threading

def BUS(i,j):
	return True

def SWITCH(i,j):
	return True

def MESH1(p):
	return lambda i,j,p=p: (i-j)**2==1

def TORUS1(p):
	return lambda i,j,p=p: (i-j+p)%p==1 or (j-i+p)%p==1

def MESH2(p):
	q=int(math.sqrt(p)+0.1)
	return lambda i,j,q=q: ((i%q-j%q)**2,(i/q-j/q)**2) in [(1,0),(0,1)]

def TORUS2(p):
	q=int(math.sqrt(p)+0.1)
	return lambda i,j,q=q: ((i%q-j%q+q)%q,(i/q-j/q+q)%q) in [(0,1),(1,0)] or \
	                       ((j%q-i%q+q)%q,(j/q-i/q+q)%q) in [(0,1),(1,0)]
def TREE(i,j):
	return i==int((j-1)/2) or j==int((i-1)/2)



dataBuffer = list()

class AsyncCoreThread(threading.Thread):
	def run(self):
		asyncore.loop()

class MessageHandler(asyncore.dispatcher):

	def handle_read(self):

		message = ""
		while True:
			data = self.recv(1024)
			self.log("Data Received :%s:" %data)

			if data == "":
				senderRank = int( message[0] )
				self.log( "sender rank = %i " % senderRank )

				data = cPickle.loads( message[1:] )
				self.log( "data receied = %s " % data )

	   			 #put the data someone sent in a list to wait
				dataBuffer.append( (senderRank, data) )			
				return	
			else:
				message = message + data


class RPSim(asyncore.dispatcher_with_send):
	def log(self,message):
		"""
		logs the message into self._logfile
		"""
		if self.logfile!=None:
			self.logfile.write(message)
		else:
			print message

	def __init__(self, nprocs, rank, mappingFile="worker.properties", topology=SWITCH, logfilename=None):
		asyncore.dispatcher.__init__(self)
		self.logfile = logfilename and open(logfilename,'w')
		self.topology = topology
		self.log("START: ")
		self.log("We are rank %i" % rank )
		self.rank = rank
		self.nprocs = nprocs
		self.log("Reading input file %s" % mappingFile)
		#create a mapping of rank -> IP, port
		self.mapping = dict()
		with open(mappingFile, "r") as f:
			for line in f:
				#don't read silly comment lines
				if line[0] == "#":
					continue
				splits = line.split(":")
				rank = int(splits[0])
				ip = splits[1]
				port = int(splits[2])
				self.mapping[rank] = (ip, port)

		self.log("Input file reading complete")
		self.log("Starting network listen....")

		myInfo = self.mapping[self.rank]
		self.log("Creating socket with info %s" % str(myInfo) )
		
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.set_reuse_addr()
		self.bind( myInfo )
		self.listen( 5 )

		self.log("Starting new thread to handle ascync core loop")
		t = AsyncCoreThread()
		t.start()	

		self.log("START: done.\n")

	#someone wants to give us data. stuff it off in our buffer
	def handle_accept(self):
		pair = self.accept()
		if pair is None:
			pass
		else:
			sock, addr = pair
			self.log("Incoming connection from %s" % repr(addr) )
			handler = MessageHandler(sock)


	def _rpsend(self,j,data):
		"""
		sends data to process #j
		"""
		if j<0: 
			self.log("process %i: send(%i,...) failed!\n" % (self.rank,j))
			raise Exception
		self.log("process %i: send(%i,%s) starting...\n" %  (self.rank,j,repr(data)))

		self.log("Ok I want to send a message to rank %i" % j )
		destination = self.mapping[j]

		self.log("We are going to then send this message to %s" % repr( destination ) )
		
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect( destination )	

		self.log("Conenction a success!")

		#TODO add some 0 prefixes
		#send my rank
		self.log( "sending my rank of %i " % self.rank )
		s.send( str(self.rank) )

		#send actual data	
		self.log("sending actual data now" )
		s.send( cPickle.dumps(data) )

		s.close()

		self.log("process %i: send(%i,%s) success.\n" %  (self.rank,j,repr(data)))

	#we want to send data. open up a connection and do so
	def rpsend(self,j,data):
		if not self.topology(self.rank,j):
			raise RuntimeError, 'topology violation'
		self._rpsend(j,data)

	def _rprecv(self,j):
		"""
		returns the data recvd from process #j
		"""
		if j < 0 or j >= self.nprocs:
			self.log("process %i: recv(%i) failed!\n" % (self.rank,j))
			raise RuntimeError
		self.log("process %i: recv(%i) starting...\n" % (self.rank,j))
	
		while True:
			for rank, data in dataBuffer:
				if rank == j:
					self.log("process %i: recv(%i) done.\n" % (self.rank,j))
					dataBuffer.remove( (rank, data) )
					return data

	#we want to get data. loop until our buffer has data in it
	def rprecv(self,j):
		self.log("receive called for rank %i" % j )
		if not self.topology(self.rank,j):
			raise RuntimeError, 'topology violation'
		return self._rprecv(j)


	def one2all_broadcast(self, source, value):
	    self.log("process %i: BEGIN one2all_broadcast(%i,%s)\n" % \
	             (self.rank,source, repr(value)))
	    if self.rank==source:
	        for i in range(0, self.nprocs):
	            if i!=source:
	                self._send(i,value)
	    else:
	        value=self._recv(source)
	    self.log("process %i: END one2all_broadcast(%i,%s)\n" % \
	             (self.rank,source, repr(value)))
	    return value

	def all2all_broadcast(self, value):
	    self.log("process %i: BEGIN all2all_broadcast(%s)\n" % \
	             (self.rank, repr(value)))
	    vector=self.all2one_collect(0,value)
	    vector=self.one2all_broadcast(0,vector)
	    self.log("process %i: END all2all_broadcast(%s)\n" % \
	             (self.rank, repr(value)))
	    return vector

	def one2all_scatter(self,source,data):
	    self.log('process %i: BEGIN all2one_scatter(%i,%s)\n' % \
	             (self.rank,source,repr(data)))
	    if self.rank==source:
	         h, reminder = divmod(len(data),self.nprocs)
	         if reminder: h+=1
	         for i in range(self.nprocs):
	             self._send(i,data[i*h:i*h+h])
	    vector = self._recv(source)
	    self.log('process %i: END all2one_scatter(%i,%s)\n' % \
	             (self.rank,source,repr(data)))
	    return vector


	def all2one_collect(self,destination,data):
	    self.log("process %i: BEGIN all2one_collect(%i,%s)\n" % \
	             (self.rank,destination,repr(data)))
	    self._send(destination,data)
	    if self.rank==destination:
	        vector = [self._recv(i) for i in range(self.nprocs)]
	    else:
	        vector = []
	    self.log("process %i: END all2one_collect(%i,%s)\n" % \
	             (self.rank,destination,repr(data)))
	    return vector

	def all2one_reduce(self,destination,value,op=lambda a,b:a+b):
	    self.log("process %i: BEGIN all2one_reduce(%s)\n" % \
	             (self.rank,repr(value)))
	    self._send(destination,value)
	    if self.rank==destination:
	        result = reduce(op,[self._recv(i) for i in range(self.nprocs)])
	    else:
	        result = None
	    self.log("process %i: END all2one_reduce(%s)\n" % \
	             (self.rank,repr(value)))
	    return result

	def all2all_reduce(self,value,op=lambda a,b:a+b):
	    self.log("process %i: BEGIN all2all_reduce(%s)\n" % \
	             (self.rank,repr(value)))
	    result=self.all2one_reduce(0,value,op)
	    result=self.one2all_broadcast(0,result)
	    self.log("process %i: END all2all_reduce(%s)\n" % \
	             (self.rank,repr(value)))
	    return result

	    @staticmethod
	    def sum(x,y): return x+y
	    @staticmethod
	    def mul(x,y): return x*y
	    @staticmethod
	    def max(x,y): return max(x,y)
	    @staticmethod
	    def min(x,y): return min(x,y)

	def barrier(self):
	    self.log("process %i: BEGIN barrier()\n" % (self.rank))
	    self.all2all_broadcast(0)
	    self.log("process %i: END barrier()\n" % (self.rank))
	    return




