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
from yarib.db.constants import MONGO_COLLECTION_RIB_PREFIX
from yarib.db.constants import MONGO_COLLECTION_RIB_ATTRIBUTE

from oslo_config import cfg

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class Route(object):

    def __init__(self):
        self.mongo_connection = self.init_mongo()
        self.create_index()
        self.clear()
        self.rib_table = {}

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

        LOG.info('Try to create index for collection %s', MONGO_COLLECTION_RIB_PREFIX)
        self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_PREFIX
        index_key_list = ['PREFIX', 'PEERADDR', 'ATTR_ID']
        for key in index_key_list:
            self.mongo_connection.get_collection().create_index(key, background=True)
        LOG.info('Try to create index for collection %s', MONGO_COLLECTION_RIB_ATTRIBUTE)

        self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_ATTRIBUTE
        index_key_list = ['PEERADDR', 'COMMUNITY', 'AS_PATH', 'ATTR']
        for key in index_key_list:
            self.mongo_connection.get_collection().create_index(key, background=True)

    @staticmethod
    def transform_attr(attr):

        attr_dict = {'ATTR': attr, 'PEERADDR': CONF.peer_ip}
        # change as path
        as_path = attr['2']
        if as_path:
            attr_dict['ORIGIN_AS'] = as_path[0][1][-1]
            attr_dict['AS_PATH'] = ' '.join(map(str, as_path[0][1])).strip()
        else:
            attr_dict['AS_PATH'] = ''
            attr_dict['ORIGIN_AS'] = ''
        # change community
        community = attr.get('8')
        if community:
            attr_dict['COMMUNITY'] = community
        else:
            attr_dict['COMMUNITY'] = []
        return attr_dict

    def update(self, attr, nlri=None, withdraw=None, insert=False, update=False):
        """
        update rib table based on the update message
        :param attr: bgp attribute dict
        :param nlri: prefix list
        :param withdraw: prefix list
        :param update: update or not
        :param insert: insert or not
        :return:
        """
        if insert:
            # try to insert all prefix in self.rib_table to database
            LOG.info('first catchup, and insert all attributes')
            if not self.rib_table:
                return
            prefix_list = []
            self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_ATTRIBUTE
            db_collection = self.mongo_connection.get_collection()
            for prefix, attr in self.rib_table.iteritems():
                find_ressult = db_collection.find_one(attr)
                if find_ressult:
                    attr_id = find_ressult['_id']
                else:
                    insert_result = db_collection.insert_one(attr)
                    attr_id = insert_result.inserted_id
                prefix_list.append({
                    'PREFIX': prefix,
                    'PEERADDR': CONF.peer_ip,
                    'ATTR_ID': attr_id
                })
            self.rib_table = {}
            LOG.info('finished insert all attributes')
            LOG.info('insert all prefixes')
            self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_PREFIX
            db_collection = self.mongo_connection.get_collection()
            db_collection.insert_many(prefix_list)
            LOG.info('finished insert all prefixes')
        # TODO (peng xiao) only support IPv4 unicast now.
        # skip none ipv4 unicast messages
        if nlri == withdraw:
            if not attr:
                # empty message
                return
            # try to get the address family
            if "14" in attr:
                if attr['14']['afi_safi'] == [25, 70]:
                    nlri = attr.pop('14')['nlri']
                    # update attribute
                    attr_dict = {'ATTR': attr, 'PEERADDR': CONF.peer_ip}
                    # change as path
                    as_path = attr['2']
                    if as_path:
                        attr_dict['ORIGIN_AS'] = as_path[0][1][-1]
                        attr_dict['AS_PATH'] = ' '.join(map(str, as_path[0][1])).strip()
                    else:
                        attr_dict['AS_PATH'] = ''
                        attr_dict['ORIGIN_AS'] = ''
                    # change community
                    community = attr.get('8')
                    if community:
                        attr_dict['COMMUNITY'] = community
                    else:
                        attr_dict['COMMUNITY'] = []
                    self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_ATTRIBUTE
                    db_collection = self.mongo_connection.get_collection()
                    find_ressult = db_collection.find_one(attr_dict)
                    if find_ressult:
                        attr_id = find_ressult['_id']
                    else:
                        upsert_result = db_collection.insert_one(attr_dict)
                        attr_id = upsert_result.inserted_id

                    self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_PREFIX
                    db_collection = self.mongo_connection.get_collection()
                    for prefix in nlri:
                        if db_collection.find_one({'PREFIX': prefix, 'PEERADDR': CONF.peer_ip}):
                            db_collection.update_one({'PREFIX': prefix, 'PEERADDR': CONF.peer_ip},
                                                     {'$set':{'ATTR_ID': attr_id}})
                        else:
                            db_collection.insert_one(
                                {'PREFIX': prefix, 'PEERADDR': CONF.peer_ip, 'ATTR_ID': attr_id})
            elif "15" in attr:
                withdraw = attr.pop('15')
            return

        # for ipv4 update
        if attr:
            attr_dict = {'ATTR': attr, 'PEERADDR': CONF.peer_ip}
            # change as path
            as_path = attr['2']
            if as_path:
                attr_dict['ORIGIN_AS'] = as_path[0][1][-1]
                attr_dict['AS_PATH'] = ' '.join(map(str, as_path[0][1])).strip()
            else:
                attr_dict['AS_PATH'] = ''
                attr_dict['ORIGIN_AS'] = ''
            # change community
            community = attr.get('8')
            if community:
                attr_dict['COMMUNITY'] = community
            else:
                attr_dict['COMMUNITY'] = []

            if insert is False and update is False:
                for prefix in nlri:
                    self.rib_table[prefix] = attr_dict
            elif update:
                # update attribute
                self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_ATTRIBUTE
                db_collection = self.mongo_connection.get_collection()
                find_ressult = db_collection.find_one(attr_dict)
                if find_ressult:
                    attr_id = find_ressult['_id']
                else:
                    upsert_result = db_collection.insert_one(attr_dict)
                    attr_id = upsert_result.inserted_id

                self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_PREFIX
                db_collection = self.mongo_connection.get_collection()
                for prefix in nlri:
                    db_collection.update_one(
                        {'PREFIX': prefix, 'PEERADDR': CONF.peer_ip}, {'ATTR_ID': attr_id}, upsert=True)

        else:
            # for withdraw
            if insert is False and update is False:
                for prefix in withdraw:
                    self.rib_table.pop(prefix)
            elif update:
                self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_PREFIX
                db_collection = self.mongo_connection.get_collection()
                for prefix in withdraw:
                    db_collection.delete_one({'PREFIX': prefix, 'PEERADDR': CONF.peer_ip})

    def clear(self):
        """
        clear rib information for this peer
        :return:
        """
        LOG.info('try to clear rib information for peer %s', CONF.peer_ip)
        self.mongo_connection.collection_name = MONGO_COLLECTION_RIB_PREFIX
        self.mongo_connection.get_collection().delete_many({'PEERADDR': CONF.peer_ip})
        self.rib_table = {}

    def search(self, sql):
        pass

    def close(self):
        LOG.info('Try to close database.')
        self.mongo_connection._close_db()
