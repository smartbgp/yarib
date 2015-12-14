# Copyright 2015 Cisco Systems, Inc.
# All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import logging

from yarib.db.mongodb import MongoApi
from yarib.db.constants import MONGO_COLLECTION_RIB_TABLE
from yarib.constants import ATTRIBUTE_ID_2_STR

from oslo_config import cfg

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class Route(object):

    def __init__(self):
        self.mongo_connection = self.init_mongo()
        self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_TABLE
        self.db_collection = self.mongo_connection.get_collection()
        self.create_index()

    @staticmethod
    def init_mongo():
        # init mongo connection
        LOG.info('Init mongodb connection.')
        if CONF.database.use_replica:
            mongo_connection = MongoApi(
                connection_url=CONF.database.connection,
                db_name=CONF.database.dbname,
                use_replica=CONF.database.use_replica,
                replica_name=CONF.database.replica_name,
                read_preference=CONF.database.read_preference,
                write_concern=CONF.database.write_concern,
                w_timeout=CONF.database.write_concern_timeout
            )
        else:
            mongo_connection = MongoApi(connection_url=CONF.database.connection, db_name=CONF.database.dbname)
        return mongo_connection

    def create_index(self):
        LOG.info('Try to create index.')
        index_key_list = ['Prefix', 'PrefixLen', 'Origin_AS', 'PeerAddress']
        for key in index_key_list:
            self.db_collection.create_index(key, background=True)

    def update(self, msg):
        """
        update rib table based on the update message
        msg example:
        {
            'ATTR',
            'WITHDRAW',
            'NLRI'
        }
        :param msg:
        :return:
        """
        # TODO (peng xiao) only support IPv4 unicast now.
        # skip none ipv4 unicast messages
        if msg['WITHDRAW'] == msg['NLRI']:
            return

        # for update
        if msg['ATTR']:
            attr_dict = {ATTRIBUTE_ID_2_STR[k]: v for k, v in msg['ATTR'].items()}
            # change as path
            as_path = msg['ATTR'][2]
            if as_path:
                attr_dict['ORIGIN_AS'] = as_path[0][1][-1]
                attr_dict[ATTRIBUTE_ID_2_STR[2]] = ' '.join(map(str, as_path[0][1])).strip()
            else:
                attr_dict[ATTRIBUTE_ID_2_STR[2]] = ''
                attr_dict['ORIGIN_AS'] = ''
        for prefix in msg['NLRI']:
            self.db_collection.update_one({'PREFIX': prefix}, {'$set': attr_dict}, upsert=True)
        # for withdraw
        for prefix in msg['WITHDRAW']:
            self.db_collection.delete_one({'PREFIX': prefix})

    def clear(self):
        """
        clear rib information for this peer
        :return:
        """
        LOG.info('try to clear rib information for peer %s', CONF.peer_ip)
        self.db_collection.delete_many({'PeerAddress': CONF.peer_ip})

    def search(self, sql):
        pass

    def close(self):
        LOG.info('Try to close database.')
        self.mongo_connection._close_db()
