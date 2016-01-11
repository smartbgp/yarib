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
import sys
import logging
import traceback

from oslo_config import cfg

from yarib import version, log
log.early_init_log(logging.DEBUG)

from yarib import config as basic_config
from yarib.db import config as db_config
from yarib.consumer import Consumer

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


def setup_basic_config():
    """
    setup and check basic configurations.
    :return:
    """
    CONF.register_cli_opts(basic_config.com_cli_opts)
    CONF.register_cli_opts(db_config.database_base_options, group='database')


def setup_advanced_config():
    if CONF.database.use_replica:
        CONF.register_opts(db_config.database_replica_options, group='database')


def prapare(args=None):

    setup_basic_config()
    try:
        CONF(args=args, project='yarib', version=version,
             default_config_files=['/etc/yarib/yarib.ini'])
    except cfg.ConfigFilesNotFoundError:
        CONF(args=args, project='yarib', version=version)
    setup_advanced_config()
    log.init_log()
    LOG.info('Log (Re)opened.')
    LOG.info("Configuration:")
    cfg.CONF.log_opt_values(LOG, logging.INFO)
    LOG.info('Starting service in PID %s' % os.getpid())

    # write pid file
    if CONF.pid_file:
        with open(CONF.pid_file, 'w') as pid_file:
            pid_file.write(str(os.getpid()))
            LOG.info('create pid file: %s' % CONF.pid_file)
    # check message file path and peer ip address
    CONF.msg_path = os.path.expanduser(CONF.msg_path)
    if not os.path.exists(os.path.join(CONF.msg_path, CONF.peer_ip, 'msg')):
        LOG.error('Message path %s does not exist!', os.path.join(CONF.msg_path, CONF.peer_ip, 'msg'))
        sys.exit()


def main():
    try:
        prapare()

        # try to get the last sequence number, if it is the first time to run, the number is 0.
        last_seq = -1
        file_consumer = Consumer(
            msg_path=os.path.join(CONF.msg_path, CONF.peer_ip, 'msg'), last_seq=last_seq, peer_ip=CONF.peer_ip)
    except Exception as e:
        print e
        sys.exit()
    try:
        file_consumer.start()
    except Exception as e:
        LOG.error(e)
        LOG.debug(traceback.format_exc(e))
    except KeyboardInterrupt:
        file_consumer.stop()
        sys.exit()
