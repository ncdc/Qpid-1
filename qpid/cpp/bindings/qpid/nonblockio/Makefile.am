#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

lib_LTLIBRARIES = libqpidnonblockio.la

NONBLOCK_IO_SOURCES = \
	qpid/messaging/Prong.cpp \
	qpid/messaging/Prong.h \
	qpid/messaging/synchio/Acknowledge.cpp \
	qpid/messaging/synchio/Acknowledge.h \
	qpid/messaging/synchio/AcknowledgeImpl.cpp \
	qpid/messaging/synchio/AcknowledgeImpl.h \
	qpid/messaging/synchio/BaseThreadedEventHandler.cpp \
	qpid/messaging/synchio/BaseThreadedEventHandler.h \
	qpid/messaging/synchio/GetOrFetchImpl.cpp \
	qpid/messaging/synchio/GetOrFetchImpl.h \
	qpid/messaging/synchio/NextReceiver.cpp \
	qpid/messaging/synchio/NextReceiver.h \
	qpid/messaging/synchio/NextReceiverImpl.cpp \
	qpid/messaging/synchio/NextReceiverImpl.h \
	qpid/messaging/synchio/ReceiverFetch.cpp \
	qpid/messaging/synchio/ReceiverFetch.h \
	qpid/messaging/synchio/ReceiverGet.cpp \
	qpid/messaging/synchio/ReceiverGet.h \
	qpid/messaging/synchio/Send.cpp \
	qpid/messaging/synchio/Send.h \
	qpid/messaging/synchio/SendImpl.cpp \
	qpid/messaging/synchio/SendImpl.h \
	qpid/messaging/synchio/SessionSync.cpp \
	qpid/messaging/synchio/SessionSync.h \
	qpid/messaging/synchio/SessionSyncImpl.cpp \
	qpid/messaging/synchio/SessionSyncImpl.h

libqpidnonblockio_la_SOURCES = $(NONBLOCK_IO_SOURCES)

libqpidnonblockio_la_LDFLAGS  = -shared
libqpidnonblockio_la_CPPFLAGS = \
	-I$(top_srcdir)/include \
	-I$(top_builddir)/include \
	-I$(top_srcdir)/src \
	-I$(top_srcdir)/bindings/qpid/nonblockio \
	-I$(top_builddir)/bindings/qpid/nonblockio
libqpidnonblockio_la_LIBADD   =

EXTRA_DIST = \
	CMakeLists.txt \
	$(NONBLOCK_IO_SOURCES)
