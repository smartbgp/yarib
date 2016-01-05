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
import unittest

from mock import MagicMock

from yarib.file import MessageFileManager


class TestFile(unittest.TestCase):

    def setUp(self):
        self.msg_path = '/test'
        os.path.exists = MagicMock(return_value=True)
        os.listdir = MagicMock(return_value=['1450672327.74.msg', '1450672676.07.msg', '1450673007.47.msg'])

    def test_locate_file_seq0(self):
        self.message_file = MessageFileManager(msgfile_dir=self.msg_path, lastseq=0)
        self.assertEqual(self.message_file.file_name, '/test/1450672327.74.msg')

    def test_locate_file_seq_neg_1(self):
        self.message_file = MessageFileManager(msgfile_dir=self.msg_path, lastseq=-1)
        self.assertEqual(self.message_file.file_name, '/test/1450673007.47.msg')

if __name__ == "__main__":
    unittest.main()
