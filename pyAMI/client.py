# -*- coding: utf-8 -*-
from __future__ import (division, print_function, unicode_literals)
#############################################################################
# Author  : Jerome ODIER, Jerome FULACHIER, Fabian LAMBERT, Solveig ALBRAND
#
# Email   : jerome.odier@lpsc.in2p3.fr
#           jerome.fulachier@lpsc.in2p3.fr
#           fabian.lambert@lpsc.in2p3.fr
#           solveig.albrand@lpsc.in2p3.fr
#
# Version : 5.X.X (2014)
#
#############################################################################

import sys, base64, getpass, pyAMI.utils, pyAMI.config, pyAMI.object, pyAMI.exception, pyAMI.transform, pyAMI.httpclient

if sys.version_info[0] == 3:
	import urllib.parse as urllib_parse
else:
	import    urllib    as urllib_parse

#############################################################################

def urllib_parse_urlencode(data):
	tmp = {}

	for key, val in list(data.items()):
		tmp[key] = val.encode('utf-8')

	return urllib_parse.urlencode(tmp)

#############################################################################

class Client(object):
	#####################################################################

	def __init__(self, endpoints, format = 'text', xslt_file = '', key_file = '', cert_file = '', ignore_proxy = False, verbose = False):
		'''Create a python AMI client.

		Args:
		    :endpoints: the list of endpoints [ str|list|tuple ]
		    :format: the default format [ str ]
		    :xslt_file: the custom XSL transformation [ str ]
		    :key_file: the key file for certificate authentication [ str ]
		    :cert_file: the cert file for certificate authentication [ str ]
		    :ignore_proxy: igonre VOMS proxy [ bool ]
		    :verbose: run in verbose mode [ bool ]
		'''

		super(Client, self).__init__()

		#############################################################

		self.config = pyAMI.config.Config(
			endpoints = endpoints,
			format = format,
			xslt_file = xslt_file,
			key_file = key_file,
			cert_file = cert_file,
			ignore_proxy = ignore_proxy
		)

		self.config.read()

		#############################################################

		self.httpClient = pyAMI.httpclient.HttpClient(self.config)

		#############################################################

		self.verbose = verbose

	#####################################################################

	def auth(self, AMIUser = None, AMIPass = None, AMIJSID = ''):
		'''Set user credentials.

		Args:
		    :AMIUser: the login [ str ]
		    :AMIPass: the password [ str ]
		    :AMIJSID: internal parameter, do not use

		Returns:
		    nothing.
		'''

		#############################################################
		# READ INFO                                                 #
		#############################################################

		if not AMIUser or not AMIPass:
			AMIUser = std_input('AMIUser: ')
			AMIPass = xxx_input('AMIPass: ')

		self.config.set('AMI', 'user', AMIUser)
		self.config.set('AMI', 'pass', AMIPass)
		self.config.set('AMI', 'jsid', AMIJSID)

		#############################################################
		# LOG-IN                                                    #
		#############################################################

		for user in self.execute('GetUserInfo -amiLogin="%s"' % AMIUser, format = 'dict_object').get_rows_i('User_Info'):

			pyAMI.utils.safeprint('  First name: %s' % user['firstName'])
			pyAMI.utils.safeprint('  Last name: %s' % user['lastName'])
			pyAMI.utils.safeprint('  Email: %s' % user['mail'])

	#####################################################################

	def reset(self):
		'''Reset user credentials.

		Returns:
		    nothing.
		'''

		self.config.reset()
		self.config.write()

	#####################################################################

	def execute(self, command, format = None, AMIUser = None, AMIPass = None, raise_errors = True):
		'''Execute an AMI command.

		Args:
		    :command: the command [ list<str> | str ]
		    :format: the format [ str ]
		    :AMIUser: override login [ str ]
		    :AMIPass: override password [ str ]
		    :raise_errors: raise exception on error [ bool ]

		Returns:
		    the command result.
		'''

		#############################################################
		# GET CONVERTER                                             #
		#############################################################

		if format is None:
			format = self.config.default_format

		try:
			converter = pyAMI.config.format_descrs[format]['converter']
			transform = pyAMI.config.format_descrs[format]['transform']

		except KeyError:
			raise pyAMI.exception.Error('issnvalid format `%s`, not in [%s]' % (
				format,
				', '.join(['`%s`' % x for x in pyAMI.config.format_descrs]),
			))

		#############################################################
		# GET AMIUser AND AMIPass                                   #
		#############################################################

		if not AMIUser\
		   or         \
		   not AMIPass:
			AMIUser = self.config.get('AMI', 'user')
			AMIPass = self.config.get('AMI', 'pass')

			AMIPass = decode(AMIPass)

		AMIUser = AMIUser.replace('\\', '\\\\').replace('"', '\\"')
		AMIPass = AMIPass.replace('\\', '\\\\').replace('"', '\\"')

		#############################################################
		# BUILD REQUEST                                             #
		#############################################################

		if isinstance(command, (list, tuple)):
			command = ' '.join(command)

		#############################################################

		data = {
			'Converter': converter,
			'Command': '%s -AMIUser="%s" -AMIPass="%s"' % (command, AMIUser, AMIPass) \
				                                                                  \
				if command                                                        \
				   and                                                            \
				   AMIUser                                                        \
				   and                                                            \
				   AMIPass else command
		}

		#############################################################

		DATA = urllib_parse_urlencode(data)

		#############################################################
		# DO REQUEST                                                #
		#############################################################

		i = 0x000000000000000000000000
		l = len(self.config.endpoints)

		while i < l:
			#####################################################
			# GET ENDPOINT                                      #
			#####################################################

			endpoint = pyAMI.config.endpoint_descrs[
				self.config.endpoints[i]
			]

			i += 1

			#####################################################
			# CONNECT                                           #
			#####################################################

			self.httpClient.connect(endpoint)

			#####################################################

			if self.verbose:
				## SAFETY ##
				data['Command'] = command
				## SAFETY ##

				pyAMI.utils.safeprint('URL     : %s://%s:%s%s?%s' % (
					self.httpClient.endpoint['prot'],
					self.httpClient.endpoint['host'],
					self.httpClient.endpoint['port'],
					self.httpClient.endpoint['path'],
					urllib_parse_urlencode(data)
				))

				pyAMI.utils.safeprint('Details :')
				pyAMI.utils.safeprint('  Session -> %s' % self.config.jsid)
				pyAMI.utils.safeprint('  Key file -> %s' % self.config.key_file)
				pyAMI.utils.safeprint('  Cert file -> %s' % self.config.cert_file)
				pyAMI.utils.safeprint('  Conn. mode -> %s' % self.config.conn_mode_str)
				pyAMI.utils.safeprint('')
				pyAMI.utils.safeprint('  Command -> %s' % data['Command'])
				pyAMI.utils.safeprint('  Converter -> %s' % data['Converter'])
				pyAMI.utils.safeprint('')

			#####################################################
			# TALK                                              #
			#####################################################

			try:
				result = self.httpClient.request(DATA).read().decode('utf-8', 'replace')

				if 'DD79E02F_E3F5_C0BF_350D_7AB71C194639' in result:
					raise ConnectionError('service temporarily unreachable')

				break

			except Exception as e:

				if i == l:
					raise e

			finally:
				self.httpClient.close()

		#############################################################
		# FORMAT RESULT                                             #
		#############################################################

		if   transform == 'custom':
			return pyAMI.transform.transform(self.config.xslt_file, result)

		elif transform == 'dom_object':
			return pyAMI.object.DOMObject(result, raise_errors = raise_errors)

		elif transform == 'dict_object':
			return pyAMI.object.DICTObject(result, raise_errors = raise_errors)

		#############################################################

		return result

#############################################################################
# SECURITY                                                                  #
#############################################################################

def safe_b64encode(s):

	if sys.version_info < (2, 7):
		s = buffer(s)

	return base64.b64encode(s)

#############################################################################

def safe_b64decode(s):

	if sys.version_info < (2, 7):
		s = buffer(s)

	return base64.b64decode(s)

#############################################################################

USER_STR = str(getpass.getuser())
USER_LEN = len(getpass.getuser())

#############################################################################

def ker(s):
	return bytearray([ord(USER_STR[i % USER_LEN]) ^ c for i, c in enumerate(bytearray(s))])

#############################################################################

def encode(s):
	return safe_b64encode(ker(s.encode('utf-8'))).decode('utf-8')

#############################################################################

def decode(s):
	return ker(safe_b64decode(s.encode('utf-8'))).decode('utf-8')

#############################################################################

def std_input(prompt = ''):

	if sys.version_info[0] == 3:
		return input(prompt)
	else:
		return raw_input(pyAMI.utils.safe_encoded_string(prompt)).decode('utf-8')

#############################################################################

def xxx_input(prompt):
	#####################################################################
	# READ PASSWORD                                                     #
	#####################################################################

	if not type(sys.stdout).__module__.startswith('IPython'):

		if sys.version_info[0] == 3:
			password = getpass.getpass(prompt)
		else:
			password = getpass.getpass(pyAMI.utils.safe_encoded_string(prompt)).decode('utf-8')

	else:
		pyAMI.utils.safeprint(prompt, endl = '')

		pyAMI.utils.safeprint('\033[40;m', endl = '')
		password = std_input()
		pyAMI.utils.safeprint('\033[0;m', endl = '')

	#####################################################################
	# ENCODE PASSWORD                                                   #
	#####################################################################

	return encode(password)

#############################################################################
