/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.logging.messages;

import java.util.List;

/**
 * Test CON Log Messages
 */
public class ConnectionMessagesTest extends AbstractTestMessages
{
    public void testConnectionOpen_WithClientIDProtocolVersionClientVersion()
    {
        String clientID = "client";
        String protocolVersion = "8-0";
        String clientVersion = "1.2.3_4";

        _logMessage = ConnectionMessages.OPEN(clientID, protocolVersion, clientVersion, true , true, true);
        List<Object> log = performLog();

        String[] expected = {"Open :", "Client ID", clientID,
                             ": Protocol Version :", protocolVersion,
                             ": Client Version :", clientVersion};

        validateLogMessage(log, "CON-1001", expected);
    }

    public void testConnectionOpen_WithClientIDNoProtocolVersionNoClientVersion()
    {
        String clientID = "client";        

        _logMessage = ConnectionMessages.OPEN(clientID, null, null, true, false, false);
        List<Object> log = performLog();

        String[] expected = {"Open :", "Client ID", clientID};

        validateLogMessage(log, "CON-1001", expected);
    }

    public void testConnectionOpen_WithNOClientIDProtocolVersionNoClientVersion()
    {
        String protocolVersion = "8-0";

        _logMessage = ConnectionMessages.OPEN(null, protocolVersion, null, false , true, false);
        List<Object> log = performLog();

        String[] expected = {"Open", ": Protocol Version :", protocolVersion};

        validateLogMessage(log, "CON-1001", expected);
    }

    public void testConnectionOpen_WithNOClientIDNoProtocolVersionClientVersion()
    {
        String clientVersion = "1.2.3_4";

        _logMessage = ConnectionMessages.OPEN(null, null, clientVersion, false , false, true);
        List<Object> log = performLog();

        String[] expected = {"Open", ": Client Version :", clientVersion};

        validateLogMessage(log, "CON-1001", expected);
    }

    public void testConnectionOpen_WithNOClientIDNoProtocolVersionNullClientVersion()
    {
        String clientVersion = null;

        _logMessage = ConnectionMessages.OPEN(null, null, clientVersion , false , false, true);
        List<Object> log = performLog();

        String[] expected = {"Open", ": Client Version :", clientVersion};

        validateLogMessage(log, "CON-1001", true, expected);
    }

    public void testConnectionOpen_WithNoClientIDNoProtocolVersionNoClientVersion()
    {
        _logMessage = ConnectionMessages.OPEN(null, null, null, false, false, false);
        List<Object> log = performLog();

        String[] expected = {"Open"};

        validateLogMessage(log, "CON-1001", expected);
    }

    public void testConnectionClose()
    {
        _logMessage = ConnectionMessages.CLOSE();
        List<Object> log = performLog();

        String[] expected = {"Close"};

        validateLogMessage(log, "CON-1002", expected);
    }

}
