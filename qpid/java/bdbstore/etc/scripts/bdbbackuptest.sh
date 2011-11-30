#!/bin/bash
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

if [ -z "$QPID_HOME" ]; then
    export QPID_HOME=$(dirname $(dirname $(readlink -f $0)))
    export PATH=${PATH}:${QPID_HOME}/bin
fi

# Parse arguements taking all - prefixed args as JAVA_OPTS
for arg in "$@"; do
    if [[ $arg == -java:* ]]; then
        JAVA_OPTS="${JAVA_OPTS}-`echo $arg|cut -d ':' -f 2`  "
    else
        ARGS="${ARGS}$arg "
    fi
done

VERSION=0.15

# Set classpath to include Qpid jar with all required jars in manifest
QPID_LIBS=$QPID_HOME/lib/qpid-all.jar:$QPID_HOME/lib/qpid-junit-toolkit-$VERSION.jar:$QPID_HOME/lib/junit-3.8.1.jar:$QPID_HOME/lib/log4j-1.2.12.jar:$QPID_HOME/lib/qpid-systests-$VERSION.jar:$QPID_HOME/lib/qpid-perftests-$VERSION.jar:$QPID_HOME/lib/slf4j-log4j12-1.6.1.jar:$QPID_HOME/lib/qpid-bdbstore-$VERSION.jar

# Set other variables used by the qpid-run script before calling
export JAVA=java        JAVA_MEM=-Xmx256m        QPID_CLASSPATH=$QPID_LIBS

. qpid-run -Dlog4j.configuration=perftests.log4j -Dbadger.level=warn -Damqj.test.logging.level=warn -Damqj.logging.level=warn ${JAVA_OPTS} org.apache.qpid.server.store.berkeleydb.testclient.BackupTestClient -o $QPID_WORK/results numMessagesToAction=55 ${ARGS}

