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

def SWITCH(i,j):
	return True


dataBuffer = list()

class AsyncCoreThread(threading.Thread):
	def run(self):
		asyncore.loop()

class MessageHandler(asyncore.dispatcher):

	def handle_read(self):

		message = ""
		while True:
			data = self.recv(1024)
			if data == "":
				senderRank = inputData[0]
				print "sender rank = ", senderRank

				data = cPickle.loads( inputData[1:] )
				print "data receied = ", data

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
			asyncore.loop()
			for rank, data in dataBuffer:
				if rank == j:
					self.log("process %i: recv(%i) done.\n" % (self.rank,j))
					dataBuffer.remove(rank, data)
					return data

	#we want to get data. loop until our buffer has data in it
	def rprecv(self,j):
		self.log("receive called for rank %i" % j )
		if not self.topology(self.rank,j):
			raise RuntimeError, 'topology violation'
		return self._rprecv(j)


