# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
Python-scoring-dag-executor
"""

from row import MutableRow
import sys
from atktypes import valid_data_types
import requests
from collections import OrderedDict
import logging


logging.basicConfig(filename='scoringPipeline.log', level=logging.DEBUG)

class Record(MutableRow):

    def add_columns(self, user_func, schema):
        logging.debug('Add')
        result = user_func(self)
        self._data = self._data + result
        if len(schema) == len(result):
            self._schema_dict.update(schema)
            for key, value in schema:
                self._dtypes.append(value)
                self._dtype_constructors.append(valid_data_types.get_constructor(value))
                self._indices_dict[key] = len(self._indices_dict)
        else:
            # log a warning and not do anything
	    logging.warn('length of schema does not match with the length of the result.')
	    pass


    def filter(self, user_func):
	logging.debug('filter')
        return user_func(self)



    def drop_columns(self, col_list):
        logging.debug('drop_columns')
        indices = []
        try:
            for i in col_list:
                indices.append(self._indices_dict[i])
                del self._schema_dict[i]

            indices.sort(reverse=True)
            for index in indices:
                del self._dtypes[index]
                del self._data[index]
                del self._dtype_constructors[index]

            self._indices_dict = dict([(k, i) for i, k, in enumerate(self._schema_dict.keys())])
        except Exception as e:
            message = "Failed to drop all columns %s" %e
	    logging.error(message)
            raise IOError(message)

    def score(self, uri):
	logging.debug('score')
        s = "http://" + uri + "/v1/score?data=" + ','.join(str(d) for d in self._data )
        return (requests.post(s).text )



    def rename_columns(self, column_list):
	logging.debug('rename_columns')
        for old_key, new_key in column_list.iteritems():
            new_dict = OrderedDict([(new_key, v) if k == old_key else (k,v) for k,v in self._schema_dict.items()])
            self._schema_dict = new_dict
        self._indices_dict = dict([(k, i) for i, k, in enumerate(self._schema_dict.keys())])















