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

import re
import os
import time
import sys
import logging
import traceback

LOG = logging.getLogger(__name__)


class MessageFileManager(object):
    """Message Manager"""

    def __init__(self, msgfile_dir, lastseq=0):

        """
        init message file object
        :param msgfile_dir: message file dir for this peer
        :param lastseq: last sequence number that read successfully
        """
        self.file_dir = msgfile_dir
        self.file_name = self._locate_file(self.file_dir, lastseq)
        self.last_line = ''
        self._f = self._locate(self.file_name, lastseq)
        self.file_list = []

    def _locate_file(self, file_dir, lastseq=0):
        """
        locate the right message file
        :param file_dir:
        :param lastseq:
        """
        LOG.info('Locate message file, when message seq = %s.' % lastseq)
        if not os.path.exists(file_dir):
            LOG.critical('The BGP data path does not exist, path=%s' % file_dir)
            sys.exit()
        self.file_list = os.listdir(file_dir)
        # delete dir
        dir_name_list = []
        for file_ in self.file_list:
            if os.path.isdir(os.path.join(file_dir, file_)):
                dir_name_list.append(file_)
        for file_ in dir_name_list:
            self.file_list.remove(file_)

        self.file_list.sort()
        if lastseq == 0:
            file_name = os.path.join(file_dir, self.file_list[0])
            LOG.info('Locate file successfully, file = %s' % file_name)
            return file_name

        find_flag = False
        for file_ in self.file_list:
            file_name = os.path.join(file_dir, file_)
            with open(file_name, 'r') as f:
                t = 0
                while True:
                    first_line = next(f)
                    try:
                        first_line = eval(first_line)
                        break

                    except Exception as e:
                        LOG.error(e)
                        ex_str = traceback.format_exc()
                        LOG.error(ex_str)
                        time.sleep(120)
                        t += 1
                        if t > 10:
                            LOG.critical("Can not get first line of %s" % file_name)
                            sys.exit()
                offs = -100
                while True:
                    f.seek(offs, 2)
                    lines = f.readlines()
                    if len(lines) > 1:
                        try:
                            last_line = lines[-1]
                            last_line = eval(last_line)
                        except Exception, e:
                            LOG.exception(e.message)
                            last_line = lines[-2]
                            try:
                                last_line = eval(last_line)
                            except Exception as e:
                                LOG.error(e)
                                ex_str = traceback.format_exc()
                                LOG.error(ex_str)
                                LOG.critical("Can not get the last line of %s" % file_name)
                                sys.exit()
                        break
                    offs *= 2
            if first_line[1] <= lastseq <= last_line[1]:
                find_flag = True
                break
        if find_flag:
            LOG.info('Locate file successfully, file = %s' % file_)
            return os.path.join(file_dir, file_)
        else:
            LOG.critical('Can not locate message file, when seq=%s' % lastseq)
            sys.exit()

    @staticmethod
    def _locate(msgfile, lastseq=0):
        """Get bgp message file handles and seek to the position after seq number your input
        """

        # Open message file
        f = open(msgfile, "r")
        LOG.info('Open BGP message file: %s' % msgfile)
        if lastseq == 0:
            return f
        else:  # skip lastseq

            # deal with first line
            patt_first = re.compile("^.*, %d," % (lastseq + 1))
            line = f.readline()
            m = patt_first.match(line)
            if m is not None:
                f.seek(0)
                return f

            # deal with following lines
            patt = re.compile("^.*, %d," % lastseq)
            while line:
                m = patt.match(line)
                if m is not None:
                    break
                line = f.readline()

            # return None is last_seq not found in msg file
            if m is None:
                return None
        return f

    @property
    def get_next_file(self):

        old_file_name = os.path.split(self.file_name)[-1]
        file_list = os.listdir(self.file_dir)
        # delete dir
        dir_name_list = []
        for file_ in file_list:
            if os.path.isdir(os.path.join(self.file_dir, file_)):
                dir_name_list.append(file_)
        for file_ in dir_name_list:
            file_list.remove(file_)
        file_list.sort()
        if len(file_list) == 1:
            return None
        index = file_list.index(old_file_name)
        if index + 1 == len(file_list):
            return None
        try:
            new_file_name = file_list[index + 1]
        except Exception as e:
            LOG.error(e)
            ex_str = traceback.format_exc()
            LOG.error(ex_str)
            return None
        return os.path.join(self.file_dir, new_file_name)

    @property
    def readline(self):
        """
        read one line and return
        return None if no new line
        """

        line = self._f.readline()
        if line and line.endswith('\n'):
            self.last_line = line
            return line
        elif line:  # this line is still be writting
            last_len = len(line)
            self._f.seek(self._f.tell() - last_len)
            return None
        else:
            time.sleep(1)
            # if need to open the next new file
            next_file = self.get_next_file
            if next_file:
                # try old file again
                line = self._f.readline()
                if line and line.endswith('\n'):
                    self.last_line = line
                    return line
                elif line:
                    last_len = len(line)
                    self._f.seek(self._f.tell() - last_len)
                    return None
                else:
                    # need to check the next file
                    with open(next_file, 'r') as f_next:
                        first_line = f_next.readline()
                        try:
                            first_seq = eval(first_line)[1]
                            last_seq = eval(self.last_line)[1]
                            LOG.debug('first seq of next file=%s, last seq of old file=%s' % (first_seq, last_seq))
                            if last_seq + 1 == first_seq:
                                # really need open next file
                                self._f.close()  # close old file
                                self._f = open(next_file, 'r')
                                LOG.info('Open next BGP message file: %s' % next_file)
                                self.file_name = next_file
                                return None
                            else:
                                f_next.close()
                                return None
                        except Exception as e:
                            LOG.error(e)
                            error_str = traceback.format_exc()
                            LOG.debug(error_str)
                            return None
            else:
                return None
