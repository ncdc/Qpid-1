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

set (qpidnonblockio_version 1.0.0)

set (qpidnonblockio_SOURCES

    qpid/messaging/Prong.cpp
    qpid/messaging/Prong.h
    qpid/messaging/TrackerAdaptor.cpp
    qpid/messaging/TrackerAdaptor.h
    qpid/messaging/TrackerAdaptorImpl.cpp
    qpid/messaging/TrackerAdaptorImpl.h
    qpid/messaging/TrackerEventHandler.h
    qpid/messaging/synchio/BaseTrackerEventHandler.cpp
    qpid/messaging/synchio/BaseTrackerEventHandler.h
    qpid/messaging/synchio/NextReceiver.cpp
    qpid/messaging/synchio/NextReceiver.h
    qpid/messaging/synchio/NextReceiverImpl.cpp
    qpid/messaging/synchio/NextReceiverImpl.h
    qpid/messaging/synchio/Receive.cpp
    qpid/messaging/synchio/Receive.h
    qpid/messaging/synchio/ReceiveImpl.cpp
    qpid/messaging/synchio/ReceiveImpl.h
    qpid/messaging/synchio/Send.cpp
    qpid/messaging/synchio/Send.h
    qpid/messaging/synchio/SendImpl.cpp
    qpid/messaging/synchio/SendImpl.h
)

add_msvc_version (qpidnonblockio library dll)
add_library(qpidnonblockio SHARED ${qpidnonblockio_SOURCES})
set_target_properties (qpidnonblockio PROPERTIES
                                      VERSION ${qpidnonblockio_version})
install(TARGETS qpidnonblockio
                DESTINATION ${QPID_INSTALL_LIBDIR}
                COMPONENT ${QPID_COMPONENT_COMMON})
install_pdb (qpidnonblockio ${QPID_COMPONENT_COMMON})

include_directories( ${CMAKE_CURRENT_SOURCE_DIR} )
include_directories( ${CMAKE_CURRENT_SOURCE_DIR}/../../../include )
include_directories( ${CMAKE_CURRENT_BINARY_DIR} )
include_directories( ${CMAKE_CURRENT_BINARY_DIR}/../../../include )

