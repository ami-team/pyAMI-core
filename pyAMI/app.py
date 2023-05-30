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

import sys, argparse, argparseplus, pyAMI.modes, pyAMI.utils, pyAMI.client, pyAMI.config, pyAMI.exception

#############################################################################

class AuthorsAction(argparse.Action):
	#####################################################################

	def __init__(self, option_strings, dest = argparse.SUPPRESS, default = argparse.SUPPRESS, help = 'show authors'):

		super(AuthorsAction, self).__init__(option_strings = option_strings, dest = dest, default = default, nargs = 0, help = help)

	#####################################################################

	def __call__(self, parser, namespace, values, option_string = None):

		parser.exit(message = '%s\n' % pyAMI.config.authors)

#############################################################################

class VersionAction(argparse.Action):
	#####################################################################

	def __init__(self, option_strings, dest = argparse.SUPPRESS, default = argparse.SUPPRESS, help = 'show version'):

		super(VersionAction, self).__init__(option_strings = option_strings, dest = dest, default = default, nargs = 0, help = help)

	#####################################################################

	def __call__(self, parser, namespace, values, option_string = None):

		parser.exit(message = '%s\n' % pyAMI.config.version)

#############################################################################

class App(object):

	def __init__(self, default_endpoint, default_format, default_xslt_file = '', default_key_file = '', default_cert_file = ''):
		#####################################################################
		# DEFAULT OPTIONS                                                   #
		#####################################################################

		parser = argparse.ArgumentParser(epilog = pyAMI.config.bug_report)

		argparseplus.patch(parser)

		parser.add_argument('--authors', action = AuthorsAction)
		parser.add_argument('--version', action = VersionAction)

		parser.add_argument('--verbose',
				dest = 'verbose', help = 'run in verbose mode', action = 'store_true', default = False)

		parser.add_argument('-e', '--endpoint',
				dest = 'endpoint', help = 'default endpoint', choices = [pyAMI.utils.safestring(x) for x in pyAMI.config.endpoint_descrs], default = default_endpoint)

		parser.add_argument('-f', '--format',
				dest = 'format', help = 'default format', choices = [pyAMI.utils.safestring(x) for x in pyAMI.config.format_descrs], default = default_format)

		parser.add_argument('-x', '--xslt',
				dest = 'xslt_file', help = 'custom XSL transformation', default = default_xslt_file)

		parser.add_argument('-k', '--key',
				dest = 'key_file', help = 'key file for certificate authentication', default = default_key_file)

		parser.add_argument('-c', '--cert',
				dest = 'cert_file', help = 'cert file for certificate authentication', default = default_cert_file)

		parser.add_argument('-i', '--ignore-proxy',
				dest = 'ignore_proxy', help = 'ignore VOMS proxy', action = 'store_true', default = False)

		subparsers = parser.add_subparsers()

		#############################################################
		# AUTH OPTIONS                                              #
		#############################################################

		subparser = subparsers.add_parser('auth', help = 'do authentication')
		subparser.set_defaults(func = pyAMI.modes.auth)

		#############################################################
		# RESET OPTIONS                                             #
		#############################################################

		subparser = subparsers.add_parser('reset', help = 'reset authentication')
		subparser.set_defaults(func = pyAMI.modes.reset)

		#############################################################
		# COMMAND OPTIONS                                           #
		#############################################################

		subparser = subparsers.add_parser('command', aliases = ['cmd'], help = 'execute a command')
		subparser.set_defaults(func = pyAMI.modes.command)

		subparser.add_argument(dest = 'amiCmdName', help = 'command', nargs = (1), metavar = 'command')
		subparser.add_argument(dest = 'amiCmdArgs', help = 'arguments', nargs = argparse.REMAINDER, metavar = '...')

		#############################################################
		# CONSOLE OPTIONS                                           #
		#############################################################

		subparser = subparsers.add_parser('console', aliases = ['con'], help = 'run the console')
		subparser.set_defaults(func = pyAMI.modes.console)

		subparser.add_argument('-i',
				dest = 'cIPython', help = 'IPython console mode', action = 'store_true', default = False)

		subparser.add_argument('-g',
				dest = 'gIPython', help = 'IPython graphical mode', action = 'store_true', default = False)

		subparser.add_argument('-l',
				dest = 'nosplash', help = 'do not show splash screen', action = 'store_true', default = False)

		subparser.add_argument(dest = 'scriptName', help = 'script', nargs = '?', metavar = 'script')
		subparser.add_argument(dest = 'scriptArgs', help = 'arguments', nargs = argparse.REMAINDER, metavar = '...')

		#############################################################

		self._parser = parser
		self._subparsers = subparsers

	#####################################################################

	@property
	def parser(self):
		return self._parser

	#####################################################################

	@property
	def subparsers(self):
		return self._subparsers

	#####################################################################

	def run(self):

		if sys.version_info[0] == 3:
			args = dict(self._parser.parse_args([                               (sys.argv[i]) for i in range(1, len(sys.argv))])._get_kwargs())
		else:
			args = dict(self._parser.parse_args([pyAMI.utils.safe_decoded_string(sys.argv[i]) for i in range(1, len(sys.argv))])._get_kwargs())

		client = pyAMI.client.Client(
			endpoints = args['endpoint'],
			format = args['format'],
			xslt_file = args['xslt_file'],
			key_file = args['key_file'],
			cert_file = args['cert_file'],
			ignore_proxy = args['ignore_proxy'],
			verbose = args['verbose']
		)

		if 'func' in args:
			func = args['func']

			del args['endpoint']
			del args['format']
			del args['xslt_file']
			del args['key_file']
			del args['cert_file']
			del args['ignore_proxy']
			del args['verbose']
			del args['func']

			try:
				return func(client, args)

			except pyAMI.exception.Error as e:
				pyAMI.utils.safeprint(e)

				return 1

		else:
			self._parser.print_help()

			return 1

#############################################################################
