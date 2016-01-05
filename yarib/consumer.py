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
import json
import traceback

from yarib.file import MessageFileManager
from yarib.db.route import Route
from yarib import constants as bgp_cons

LOG = logging.getLogger(__name__)


class Consumer(object):

    def __init__(self, msg_path, peer_ip, last_seq=0):

        self.file_handler = MessageFileManager(msg_path, last_seq)
        self.peer_ip = peer_ip
        self.rib_handler = Route()
        self.first_time_catchup_flag = False

    def start(self):

        update = False
        insert = False
        while True:
            line = self.file_handler.readline
            if not line:
                if not self.first_time_catchup_flag:
                    self.first_time_catchup_flag = True
                    self.rib_handler.update(attr=None, insert=True)
                    # after that, set update True
                    update = True
                continue
            try:
                bgp_msg = json.loads(line)
            except Exception as e:
                LOG.critical('Message format error when using eval, line = %s detail: %s' % (line, e))
                continue
            # message type
            if bgp_msg['type'] == bgp_cons.BGP_UPDATE:
                try:
                    self.rib_handler.update(
                        attr=bgp_msg['attr'], nlri=bgp_msg['nlri'], withdraw=bgp_msg['withdraw'],
                        update=update, insert=insert)
                except Exception as e:
                    LOG.error(e)
                    LOG.debug(traceback.format_exc())
            elif bgp_msg['type'] in [bgp_cons.BGP_NOTIFICATION, bgp_cons.BGP_OPEN]:
                self.rib_handler.clear()

    def stop(self):
        pass
