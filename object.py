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

import abc, sys, json, xml.dom.minidom, pyAMI.utils, pyAMI.exception

#############################################################################

try:
	from collections import OrderedDict as pyAMIDict

except ImportError:
	pyAMIDict = dict

#############################################################################

@pyAMI.utils.metaclass(abc.ABCMeta)
class AMIObject(object):
	#####################################################################

	def __init__(self):
		super(AMIObject, self).__init__()

		#############################################################

		self.data = None

		self.entity = None

		self.infos = []
		self.errors = []
		self.rowsets = []

		self.formated_data = ''

	#####################################################################

	@abc.abstractmethod
	def get_rows_i(self, rowset_type = None):
		pass

	#####################################################################

	@abc.abstractmethod
	def get_rowset_types_i(self):
		pass

	#####################################################################

	def get_rows(self, rowset_type = None):
		return list(self.get_rows_i(rowset_type))

	#####################################################################

	def get_rowset_types(self):
		result = []

		for rowset_type in self.get_rowset_types_i():

			if not rowset_type in result:
				result.append(rowset_type)

		return result

	#####################################################################

	def __str__(self):
		return pyAMI.utils.safestring(self.formated_data)

#############################################################################

class DOMObject(AMIObject):
	#####################################################################

	def __init__(self, raw_data, raise_errors = True):
		super(DOMObject, self).__init__()

		#############################################################
		# PARSE XML                                                 #
		#############################################################

		try:
			self.data = xml.dom.minidom.parseString(raw_data.encode('utf-8'))

		except Exception:
			raise pyAMI.exception.Error('command parsing error, please contact ami@lpsc.in2p3.fr')

		#############################################################

		try:
			for arg in self.data.getElementsByTagName('args'):

				if arg.attributes['argName'].value == 'entity':
					self.entity = arg.attributes['argValue'].value
					break

		except (KeyError, IndexError):
			pass

		#############################################################

		self.infos = self.data.getElementsByTagName('info')

		self.errors = self.data.getElementsByTagName('error')

		self.rowsets = self.data.getElementsByTagName('rowset')

		#############################################################
		# GENERATE INDENTED RAW DATA                                #
		#############################################################

		self.formated_data = '\n'.join([line for line in self.data.toprettyxml(indent = '  ').replace(']]>', ']]>\n').split('\n') if line.strip()])

		#############################################################
		# CHECK ERRORS                                              #
		#############################################################

		if raise_errors:
			messages = []

			for error in self.errors:

				try:
					messages.append(error.firstChild.nodeValue)
				except:
					pass

			if messages:
				raise pyAMI.exception.Error('\n'.join(messages))

	#####################################################################

	def get_rows_i(self, rowset_type = None):

		for rowset in self.rowsets:

			if not rowset_type or (((sys.version_info[0] == 3 and rowset.hasAttribute('type')) or (sys.version_info[0] == 2 and rowset.attributes.has_key('type'))) and rowset.attributes['type'].value == rowset_type):

				for row in rowset.getElementsByTagName('row'):
					field_dict = pyAMIDict()

					for field in row.getElementsByTagName('field'):
						name = field.attributes['name'].value

						if (sys.version_info[0] == 3 and field.hasAttribute('table')) or (sys.version_info[0] == 2 and field.attributes.has_key('table')):
							table = field.attributes['table'].value

							if self.entity and table and self.entity != table:
								name = '%s.%s' % (table, name)

						if field.firstChild:
							field_dict[name] = field.firstChild.nodeValue
						else:
							field_dict[name] = 'NULL'

					yield field_dict

	#####################################################################

	def get_rowset_types_i(self):

		for rowset in self.rowsets:

			yield rowset.attributes['type'].value

#############################################################################

class DICTObject(AMIObject):
	#####################################################################

	def __init__(self, raw_data, raise_errors = True):
		super(DICTObject, self).__init__()

		#############################################################
		# PARSE JSON                                                #
		#############################################################

		try:
			self.data = json.loads(raw_data)

		except Exception:
			raise pyAMI.exception.Error('command parsing error, please contact ami@lpsc.in2p3.fr')

		#############################################################

		try:
			for arg in self.data['AMIMessage'][0]['commandArgs'][0]['args']:

				if arg['@argName'] == 'entity':
					self.entity = arg['@argValue']
					break

		except (KeyError, IndexError):
			pass

		#############################################################

		try:
			self.infos = self.data['AMIMessage'][0]['info']

		except (KeyError, IndexError):
			pass

		try:
			self.errors = self.data['AMIMessage'][0]['error']

		except (KeyError, IndexError):
			pass

		try:
			self.rowsets = self.data['AMIMessage'][0]['Result'][0]['rowset']

		except (KeyError, IndexError):
			pass

		#############################################################
		# GENERATE INDENTED RAW DATA                                #
		#############################################################

		self.formated_data = json.dumps(self.data, indent = 2, sort_keys = False)

		#############################################################
		# CHECK ERRORS                                              #
		#############################################################

		if raise_errors:
			messages = []

			for error in self.errors:

				try:
					messages.append(error['$'])
				except:
					pass

			if messages:
				raise pyAMI.exception.Error('\n'.join(messages))

	#####################################################################

	def get_rows_i(self, rowset_type = None):

		for rowset in self.rowsets:

			if not rowset_type or ('@type' in rowset and rowset['@type'] == rowset_type):

				if 'row' in rowset:
					for row in rowset['row']:
						field_dict = pyAMIDict()

						if 'field' in row:
							for field in row['field']:
								name = field['@name']

								if '@table' in field:
									table = field['@table']

									if self.entity and table and self.entity != table:
										name = '%s.%s' % (table, name)

								if '$' in field:
									field_dict[name] = field['$']
								else:
									field_dict[name] = 'NULL'

						yield field_dict

	#####################################################################

	def get_rowset_types_i(self):

		for rowset in self.rowsets:

			yield rowset['@type']

#############################################################################
