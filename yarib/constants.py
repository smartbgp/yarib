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

# BGP message types
BGP_OPEN = 1
BGP_UPDATE = 2
BGP_NOTIFICATION = 3
BGP_KEEPALIVE = 4
BGP_ROUTE_REFRESH = 5

ATTRIBUTE_ID_2_STR = {
    1: 'ORIGIN',
    2: 'AS_PATH',
    3: 'NEXT_HOP',
    4: 'MULTI_EXIT_DISC',
    5: 'LOCAL_PREF',
    6: 'ATOMIC_AGGREGATE',
    7: 'AGGREGATOR',
    8: 'COMMUNITY',
    9: 'ORIGINATOR_ID',
    10: 'CLUSTER_LIST',
    14: 'MP_REACH_NLRI',
    15: 'MP_UNREACH_NLRI',
    16: 'EXTENDED_COMMUNITY',
    17: 'AS4_PATH',
    18: 'AS4_AGGREGATOR'
}
