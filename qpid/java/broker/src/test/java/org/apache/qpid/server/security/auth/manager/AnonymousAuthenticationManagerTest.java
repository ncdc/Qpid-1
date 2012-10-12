/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.server.security.auth.manager;

import static org.apache.qpid.server.security.auth.AuthenticatedPrincipalTestHelper.assertOnlyContainsWrapped;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.test.utils.QpidTestCase;

public class AnonymousAuthenticationManagerTest extends QpidTestCase
{
    private AuthenticationManager _manager = new AnonymousAuthenticationManager();

    public void tearDown() throws Exception
    {
        if(_manager != null)
        {
            _manager = null;
        }
    }

    public void testGetMechanisms() throws Exception
    {
        assertEquals("ANONYMOUS", _manager.getMechanisms());
    }

    public void testCreateSaslServer() throws Exception
    {
        SaslServer server = _manager.createSaslServer("ANONYMOUS", "example.example.com", null);

        assertEquals("Sasl Server mechanism name is not as expected", "ANONYMOUS", server.getMechanismName());

        try
        {
            server = _manager.createSaslServer("PLAIN", "example.example.com", null);
            fail("Expected creating SaslServer with incorrect mechanism to throw an exception");
        }
        catch (SaslException e)
        {
            // pass
        }
    }

    public void testAuthenticate() throws Exception
    {
        SaslServer saslServer = _manager.createSaslServer("ANONYMOUS", "example.example.com", null);
        AuthenticationResult result = _manager.authenticate(saslServer, new byte[0]);
        assertNotNull(result);
        assertEquals("Expected authentication to be successful",
                     AuthenticationResult.AuthenticationStatus.SUCCESS,
                     result.getStatus());

        assertOnlyContainsWrapped(AnonymousAuthenticationManager.ANONYMOUS_PRINCIPAL, result.getPrincipals());
    }


}
