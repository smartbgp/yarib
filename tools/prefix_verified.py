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
import os
import time
import logging

from pymongo import MongoClient

CONFIG = {}
MONGO_COLLECTION_BGP_AGENT = 'BGP_AGENT'
MONGO_COLLECTION_RIB_PREFIX = 'RIB_PREFIX'
MONGO_COLLECTION_RIB_ATTRIBUTE = 'RIB_ATTRIBUTE'
MONGO_COLLECTION_ACT_PREFIX = 'ACT_PREFIX'
MONGO_COLLECTION_ROUTE_POLICY = 'ROUTE_POLICY'

# mongodb
CONFIG['DB_NAME'] = os.environ.get('MONGODB_NAME', 'yabgp')
CONFIG['DB_URL'] = os.environ.get('MONGODB_URL', 'mongodb://yabgp:yabgp@as-gerrit.cisco.com:27017,as-gerrit.cisco.com:27018,as-gerrit.cisco.com:27019')

# logging
handler = logging.FileHandler('prefix_verified.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
LOG.addHandler(handler)


# database
db_client = MongoClient(host=CONFIG['DB_URL'])
dbh = db_client[CONFIG['DB_NAME']]


def main():
    LOG.info('begin to check')
    total_policy = dbh[MONGO_COLLECTION_ROUTE_POLICY].find()
    LOG.info('There are %s route policies in db', total_policy.count())
    running_count = 0
    verified_count = 0
    for policy in total_policy:
        verified = True
        if policy['status'] != 7:
            # if the policy is not running, make sure the verifed value is False
            verified = False
        else:
            running_count += 1
            # try to verify the policy
            all_act_prefix = dbh[MONGO_COLLECTION_ACT_PREFIX].find({'policy_id': policy['_id']})
            for act_prefix in all_act_prefix:
                if act_prefix.get('afi_safi') == 'ipv4':
                    prefix = act_prefix['prefix_send']
                    nexthop = act_prefix['new_attr'].get('3')
                    local_pre = act_prefix['new_attr'].get('5')
                    prefix_find = dbh[MONGO_COLLECTION_RIB_PREFIX].find_one({'PREFIX': prefix})
                    if afi_safi == 'ipv4':
                        if not prefix_find:
                            verified = False
                            break
                    
                    else:
                        # check attribute
                        attribute_find = dbh[MONGO_COLLECTION_RIB_ATTRIBUTE].find_one({'_id': prefix_find['ATTR_ID']})
                        if not attribute_find:
                            verified = False
                        else:
                            # check nexthop and local_pre
                            if nexthop != attribute_find['ATTR']['3']:
                                verified = False
                                LOG.info('prefix %s nexthop diff %s %s', prefix, nexthop, attribute_find['ATTR']['3'])
                            if local_pre != attribute_find['ATTR']['5']:
                                verified = False
                                LOG.info('prefix %s local pre diff %s %s', prefix, local_pre, attribute_find['ATTR']['5'])
                else:
                    verified = False
        if verified:
            verified_count += 1
            # change the policy's verified flag from false to true
            dbh[MONGO_COLLECTION_ROUTE_POLICY].update_one(
                {'_id': policy['_id']}, {'$set': {'verified': True}})
    LOG.info('--- %s of %s are running', running_count, total_policy.count())
    LOG.info('--- %s of %s are verified', verified_count, total_policy.count())


if __name__ == '__main__':
    main()
