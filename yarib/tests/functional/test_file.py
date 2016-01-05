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

from yarib.file import MessageFileManager

f1 = MessageFileManager(msgfile_dir='/Users/penxiao/data/bgp/10.124.1.221/msg', lastseq=0)
print f1.file_name, f1.readline

f2 = MessageFileManager(msgfile_dir='/Users/penxiao/data/bgp/10.124.1.221/msg', lastseq=-1)
print f2.file_name, f2.readline

f3 = MessageFileManager(msgfile_dir='/Users/penxiao/data/bgp/10.124.1.221/msg', lastseq=1425104)

print f3.file_name, f3.readline
