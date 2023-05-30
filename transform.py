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

import pyAMI.exception

#############################################################################

def transform(xsl, xml):
	#####################################################################
	# IMPORT TINY_XSLT                                                  #
	#####################################################################

	try:
		import tiny_xslt

	except ImportError as e:
		raise pyAMI.exception.Error('module `tiny_xslt` not properly installed: %s' % e)

	#####################################################################
	# OPEN CUSTOM XSL                                                   #
	#####################################################################

	try:
		f = open(xsl, 'r')
		xsl = f.read()
		f.close()

	except IOError:
		raise pyAMI.exception.Error('could not open xslt file `%s`' % xsl)

	#####################################################################
	# APPLY XSL                                                         #
	#####################################################################

	try:
		return tiny_xslt.transform(xsl, xml)

	except tiny_xslt.Error as e:
		raise pyAMI.exception.Error(e)

#############################################################################
