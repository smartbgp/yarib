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

from yarib.file import MessageFileManager
from yarib.db.route import Route
from yarib import constants as bgp_cons

LOG = logging.getLogger(__name__)


class Consumer(object):

    def __init__(self, msg_path, peer_ip, last_seq=0):

        self.file_handler = MessageFileManager(msg_path, last_seq)
        self.peer_ip = peer_ip
        self.rib_handler = Route()

    def start(self):

        while True:
            line = self.file_handler.readline
            if not line:
                continue
            try:
                bgp_msg = eval(line)
            except Exception as e:
                LOG.critical('Message format error when using eval, line = %s detail: %s' % (line, e))
                continue
            # message type
            if bgp_msg[2] == bgp_cons.BGP_UPDATE:
                try:
                    self.rib_handler.update(bgp_msg[3])
                except Exception as e:
                    LOG.error(e)
                    LOG.info(str(bgp_msg[3]))
            elif bgp_msg[2] in [bgp_cons.BGP_NOTIFICATION, bgp_cons.BGP_OPEN]:
                self.rib_handler.clear()
            self.update_seq_file(bgp_msg[1])

    def update_seq_file(self, seq):
        with open('seq-%s' % self.peer_ip, 'w') as f:
            f.write(str(seq))

    def stop(self):
        pass
