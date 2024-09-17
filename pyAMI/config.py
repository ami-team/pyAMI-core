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

import os, sys, zlib, pyAMI.exception

if sys.version_info[0] == 3:
	import configparser as configparser
else:
	import ConfigParser as configparser

#############################################################################
# GLOBAL DEFINITIONS                                                        #
#############################################################################

version = '5.1.7'

#############################################################################

banner = r'''
              _   __  __ ___    ___
  _ __ _  _  / \ |  \/  |_ _|  | __|
 | '_ \ || |/ ^ \| |\/| || |   |__ \
 | .__/\_, /_/ \_\_|  |_|___|  |___/%s
 |_|   |__/ ami@lpsc.in2p3.fr
'''[1: ] % version[version.find('.'): ]

#############################################################################

author_array = [
	{'name': 'Jerome ODIER', 'email': 'jerome.odier@lpsc.in2p3.fr'},
	{'name': 'Jerome FULACHIER', 'email': 'jerome.fulachier@lpsc.in2p3.fr'},
	{'name': 'Fabian LAMBERT', 'email': 'fabian.lambert@lpsc.in2p3.fr'},
	{'name': 'Solveig ALBRAND', 'email': 'solveig.albrand@lpsc.in2p3.fr'},
]

author_names = ', '.join([author['name'] for author in author_array])
author_emails = ', '.join([author['email'] for author in author_array])

authors = '\n'.join(['%s, %s' % (author['name'], author['email']) for author in author_array])

#############################################################################

bug_report = 'Bug reports: ami@lpsc.in2p3.fr'

#############################################################################

endpoint_descrs = {
	'local': {'prot': 'https', 'host': 'localhost', 'port': '8443', 'path': '/AMI/FrontEnd'},
}

#############################################################################

format_descrs = {
	'xml': {'converter': '', 'transform': ''},
	'custom': {'converter': '', 'transform': 'custom'},
	'dom_object': {'converter': '', 'transform': 'dom_object'},
	'dict_object': {'converter': 'AMIXmlToJson.xsl', 'transform': 'dict_object'},
	'json': {'converter': 'AMIXmlToJson.xsl', 'transform': ''},
	'text': {'converter': 'AMIXmlToText.xsl', 'transform': ''},
	'csv': {'converter': 'AMIXmlToCsv.xsl', 'transform': ''},
}

#############################################################################

tables = {}

#############################################################################

file_system_encoding = sys.getfilesystemencoding() if not sys.getfilesystemencoding() is None else 'utf-8'
console_encoding     = sys.stdout.encoding         if not sys.stdout.encoding         is None else 'utf-8'

#############################################################################

home_path = os.path.expanduser(b'~').decode(file_system_encoding)

config_path = os.getenv('PYAMI_CONFIG_DIR', home_path + os.sep + '.pyami')

config_file = config_path + os.sep + 'pyami.cfg'

#############################################################################
# USER DEFINITIONS                                                          #
#############################################################################

class Config(object):
	#####################################################################

	CONN_MODE_LOGIN = 0
	CONN_MODE_CERT = 1
	CONN_MODE_PROXY = 2

	#####################################################################

	def __init__(self, endpoints, format = 'text', xslt_file = '', key_file = '', cert_file = '', ignore_proxy = False):
		super(Config, self).__init__()

		#############################################################

		try:
			self._ini = configparser.ConfigParser(interpolation = None)
		except TypeError:
			self._ini = configparser.ConfigParser()

		self._ini.add_section('AMI')

		self.set = self._ini.set
		self.get = self._ini.get

		#############################################################

		if not isinstance(endpoints, list) \
		   and                             \
		   not isinstance(endpoints, tuple):
			endpoints = [endpoints]

		#############################################################

		for endpoint in endpoints:

			if not endpoint in endpoint_descrs:

				raise pyAMI.exception.Error('invalid endpoint `%s`, not in [%s]' % (
					endpoint,
					', '.join(['`%s`' % x for x in endpoint_descrs]),
				))

		#############################################################

		self.default_endpoints = endpoints
		self.default_format = format
		self.default_xslt_file = xslt_file
		self.default_key_file = key_file
		self.default_cert_file = cert_file

		self.ignore_proxy = ignore_proxy

		self.endpoints = None
		self.format = None
		self.xslt_file = None
		self.cert_file = None
		self.key_file = None

		self.conn_mode = None

		#############################################################

		self.reset()

	#####################################################################

	def reset(self):
		#############################################################
		# FLUSH CONFIG FILE                                         #
		#############################################################

		self.set('AMI', 'user', '')
		self.set('AMI', 'pass', '')
		self.set('AMI', 'jsid', '')
		self.set('AMI', 'hash', '')

		#############################################################
		# SET ENDPOINT AND XSLT_FILE                                #
		#############################################################

		self.endpoints = self.default_endpoints
		self.format = self.default_format
		self.xslt_file = self.default_xslt_file

		#############################################################
		# SET KEY_FILE AND CERT_FILE                                #
		#############################################################

		if os.path.isfile(self.default_key_file)\
		   and                                   \
		   os.path.isfile(self.default_cert_file):

			self.key_file = self.default_key_file
			self.cert_file = self.default_cert_file

			self.conn_mode = Config.CONN_MODE_CERT

		else:
			uid = os.geteuid() if hasattr(os, 'geteuid') else -1

			proxy_file = os.getenv('X509_USER_PROXY', '/tmp/x509up_u%d' % uid)

			if not self.ignore_proxy and os.path.isfile(proxy_file):
				self.key_file = proxy_file
				self.cert_file = proxy_file

				self.conn_mode = Config.CONN_MODE_PROXY

			else:
				self.key_file = ((((''))))
				self.cert_file = ((((''))))

				self.conn_mode = Config.CONN_MODE_LOGIN

	#####################################################################

	def read(self):
		self._ini.read(config_file)

		if self.conn_mode == Config.CONN_MODE_LOGIN:
			ah = '%08X' % (zlib.adler32((((((((((('')))))))))).encode()) & 0xFFFFFFFF)
		else:
			ah = '%08X' % (zlib.adler32(self.cert_file_content.encode()) & 0xFFFFFFFF)

		if self.get('AMI', 'hash') != ah:

			self.set('AMI', 'jsid', '')
			self.set('AMI', 'hash', ah)

	#####################################################################

	def write(self):

		try:
			#####################################################
			# MKDIR ${PYAMI_CONFIG_DIR}/.pyami                  #
			#####################################################

			if not os.path.isdir(config_path):
				os.makedirs(config_path, 0x1C0)

			#####################################################
			# WRITE ${PYAMI_CONFIG_DIR}/.pyami/ami.cfg          #
			#####################################################

			fp = open(config_file, 'w')
			fp.write('; Please do not share this file.\n')
			self._ini.write(fp)
			fp.close()

			#####################################################
			# PROTECT ${PYAMI_CONFIG_DIR}/.pyami/ami.cfg        #
			#####################################################

			os.chmod(config_file, 0x180)

			#####################################################

		except (OSError, IOError) as e:
			raise pyAMI.exception.Error(e)

	#####################################################################

	@property
	def jsid(self):
		return self.get('AMI', 'jsid')

	#####################################################################

	@jsid.setter
	def jsid(self, jsid):

		if jsid and jsid != self.get('AMI', 'jsid'):

			self.set('AMI', 'jsid', jsid)

			self.write()

	#####################################################################

	@property
	def cert_file_content(self):
		#############################################################
		# LOAD CERTIFICATE                                          #
		#############################################################

		try:
			fp = open(self.cert_file, 'r')
			result = fp.read()
			fp.close()

		except IOError:
			raise pyAMI.exception.Error('could not open certificate or proxy `%s`' % self.cert_file)

		#############################################################

		return result

	#####################################################################

	@property
	def conn_mode_str(self):

		if   self.conn_mode == Config.CONN_MODE_LOGIN:
			result = 'LOGIN'
		elif self.conn_mode == Config.CONN_MODE_CERT:
			result = 'CERT'
		elif self.conn_mode == Config.CONN_MODE_PROXY:
			result = 'PROXY'
		else:
			result = '????'

		return result

#############################################################################
