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


class RPSim(asyncore.dispatcher):
	def log(self,message):
		"""
		logs the message into self._logfile
		"""
		if self.logfile!=None:
			self.logfile.write(message)
		else:
			print message

	def __init__(self, rank, mappingFile, topology=SWITCH, logfilename=None):
	    asyncore.dispatcher.__init__(self)
		self.logfile = logfilename and open(logfilename,'w')
		self.topology = topology
		self.log("START: ")
		self.log("We are rank %i" % rank )
		self.rank = rank
		self.log("Reading input file %s" % mappingFile)
		self.mapping = dict()
		with open(mappingFile, "r") as f:
			for line in f:
				#don't read silly comment lines
				if line[0] == "#":
					continue
				splits = line.split(":")
				rank = int(splits[0])
				ip = splits[1]
				port = int(splits[2]
				self.mapping[rank] = ip, port

		self.log("Input file reading complete")
		self.log("Starting network listen....")

		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		selfInfo = self.mapping[self.rank]
		self.log("Creating socket with info %s" % str(selfInfo)
		self.connect( selfInfo )

		self.log("START: done.\n")

	def handle_close(self):
		self.close()

	def handle_read(self):
		self.recv(8192)


asyncore.loop()

