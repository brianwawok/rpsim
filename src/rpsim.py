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


def SWITCH(i,j):
	return True


dataBuffer = list()

class MessageHandler(asyncore.dispatcher_with_send):

	def handle_read(self):
		senderRank = self.recv(4)
		print "sender rank = ", senderRank

		dataSize = self.recv(4)
		print "data length = ", dataSize

		data = cPickle.load( self.recv(dataSize) )
		print "data receied = ", data

		dataBuffer.append( (senderRank, data) )


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

		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		myInfo = self.mapping[self.rank]
		self.log("Creating socket with info %s" % str(myInfo) )
		self.connect( myInfo )

		self.log("START: done.\n")

	def handle_connect(self):
		pass

	def handle_close(self):
		self.close()

	def handle_read(self):
		print self.recv(8192)

	def writable(self):
		return (len(self.buffer) > 0)

   	def handle_write(self):
		sent = self.send(self.buffer)
		self.buffer = self.buffer[sent:]


	#someone wants to give us data. stuff it off in our buffer
	def handle_accept(self):
		pair = self.accept()
		if pair is None:
			pass
		else:
			sock, addr = pair
			self.log("Incoming connection from %s" % repr(addr) )
			handler = MessageHandler(sock)


	def _send(self,j,data):
		"""
		sends data to process #j
		"""
		if j<0: 
			self.log("process %i: send(%i,...) failed!\n" % (self.rank,j))
			raise Exception
		self.log("process %i: send(%i,%s) starting...\n" %  (self.rank,j,repr(data)))
		s = cPickle.dumps(data)

		self.log("Ok I want to send a message to rank %i" % j )
		destination = self.mapping[j]

		self.log("We are going to then send this message to %s" % repr( destination ) )
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect( destination )	

		#send my ran
		self.log( "sending my rank of %i " % self.rank )
		self.send( str(self.rank) )

		#send data length
		self.log("sending data length of %i " % len( s ))
		s = cPickle.dump(data)
		self.send( str( len( s ) ) )		

		#send actual data	
		self.log("sending actual data now" )
		self.send( s )


		self.log("process %i: send(%i,%s) success.\n" %  (self.rank,j,repr(data)))

	#we want to send data. open up a connection and do so
	def rpsend(self,j,data):
		if not self.topology(self.rank,j):
			raise RuntimeError, 'topology violation'
		self._send(j,data)

	def _recv(self,j):
		"""
		returns the data recvd from process #j
		"""
		if j<0 or j>=self.nprocs:
			self.log("process %i: recv(%i) failed!\n" % (self.rank,j))
			raise RuntimeError
		self.log("process %i: recv(%i) starting...\n" % (self.rank,j))
		
		while True:
			for rank, data in dataBuffer:
				if rank == j:
					self.log("process %i: recv(%i) done.\n" % (self.rank,j))
					dataBuffer.remove(rank, data)
					return data
			#not found, sleep for a bit
			time.sleep(1)

	#we want to get data. loop until our buffer has data in it
	def rprecv(self,j):
		if not self.topology(self.rank,j):
			raise RuntimeError, 'topology violation'
		return self._recv(j)



