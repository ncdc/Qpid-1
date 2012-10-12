/*
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
 */
package org.apache.qpid.server.security.auth.manager;

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;

public class KerberosAuthenticationManager implements AuthenticationManager
{
    private static final String GSSAPI_MECHANISM = "GSSAPI";
    private final CallbackHandler _callbackHandler = new GssApiCallbackHandler();

    KerberosAuthenticationManager()
    {
    }

    @Override
    public void initialise()
    {

    }

    @Override
    public String getMechanisms()
    {
        return GSSAPI_MECHANISM;
    }

    @Override
    public SaslServer createSaslServer(String mechanism, String localFQDN, Principal externalPrincipal) throws SaslException
    {
        if(GSSAPI_MECHANISM.equals(mechanism))
        {
            try
            {
            return Sasl.createSaslServer(GSSAPI_MECHANISM, "AMQP", localFQDN,
                                         new HashMap<String, Object>(), _callbackHandler);
            }
            catch (SaslException e)
            {
                e.printStackTrace(System.err);
                throw e;
            }
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
                return new AuthenticationResult(new UsernamePrincipal(server.getAuthorizationID()));
            }
            else
            {
                return new AuthenticationResult(challenge, AuthenticationResult.AuthenticationStatus.CONTINUE);
            }
        }
        catch (SaslException e)
        {
            e.printStackTrace(System.err);
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    @Override
    public AuthenticationResult authenticate(String username, String password)
    {
        return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
    }

    @Override
    public void close()
    {
    }

    private static class GssApiCallbackHandler implements CallbackHandler
    {

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
        {
            for(Callback callback : callbacks)
            {
                if (callback instanceof AuthorizeCallback)
                {
                    ((AuthorizeCallback) callback).setAuthorized(true);
                }
                else
                {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }
}
