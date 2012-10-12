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

import java.security.Principal;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.anonymous.AnonymousInitialiser;
import org.apache.qpid.server.security.auth.sasl.anonymous.AnonymousSaslServer;

public class AnonymousAuthenticationManager implements AuthenticationManager
{
    private static final AnonymousInitialiser SASL_INITIALISER = new AnonymousInitialiser();

    private static final String ANONYMOUS = SASL_INITIALISER.getMechanismName();

    public static final String ANONYMOUS_USERNAME = "ANONYMOUS";

    public static final Principal ANONYMOUS_PRINCIPAL = new UsernamePrincipal(ANONYMOUS_USERNAME);

    public static final Subject ANONYMOUS_SUBJECT = new Subject();
    static
    {
        ANONYMOUS_SUBJECT.getPrincipals().add(ANONYMOUS_PRINCIPAL);
    }

    private static final AuthenticationResult ANONYMOUS_AUTHENTICATION = new AuthenticationResult(ANONYMOUS_PRINCIPAL);

    static final AnonymousAuthenticationManager INSTANCE = new AnonymousAuthenticationManager();

    AnonymousAuthenticationManager()
    {
    }

    @Override
    public void initialise()
    {

    }

    @Override
    public String getMechanisms()
    {
        return ANONYMOUS;
    }

    @Override
    public SaslServer createSaslServer(String mechanism, String localFQDN, Principal externalPrincipal) throws SaslException
    {
        if(ANONYMOUS.equals(mechanism))
        {
            return new AnonymousSaslServer();
        }
        else
        {
            throw new SaslException("Unknown mechanism: " + mechanism);
        }
    }

    @Override
    public AuthenticationResult authenticate(SaslServer server, byte[] response)
    {
        try
        {
            // Process response from the client
            byte[] challenge = server.evaluateResponse(response != null ? response : new byte[0]);

            if (server.isComplete())
            {
                return ANONYMOUS_AUTHENTICATION;
            }
            else
            {
                return new AuthenticationResult(challenge, AuthenticationResult.AuthenticationStatus.CONTINUE);
            }
        }
        catch (SaslException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    @Override
    public AuthenticationResult authenticate(String username, String password)
    {
        return ANONYMOUS_AUTHENTICATION;
    }

    @Override
    public void close()
    {
    }
}
