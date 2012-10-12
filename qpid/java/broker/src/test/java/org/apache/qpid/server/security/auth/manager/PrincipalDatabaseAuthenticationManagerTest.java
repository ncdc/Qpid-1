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
package org.apache.qpid.server.security.auth.manager;

import static org.apache.qpid.server.security.auth.AuthenticatedPrincipalTestHelper.assertOnlyContainsWrapped;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Provider;
import java.security.Security;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.database.PrincipalDatabase;
import org.apache.qpid.server.security.auth.sasl.AuthenticationProviderInitialiser;
import org.apache.qpid.server.security.auth.sasl.UsernamePasswordInitialiser;
import org.apache.qpid.test.utils.QpidTestCase;

/**
 * Tests the public methods of PrincipalDatabaseAuthenticationManager.
 *
 */
public class PrincipalDatabaseAuthenticationManagerTest extends QpidTestCase
{
    private static final String MOCK_MECH_NAME = "MOCK-MECH-NAME";
    private static final UsernamePrincipal PRINCIPAL = new UsernamePrincipal("guest");

    private AuthenticationManager _manager = null; // Class under test
    private PrincipalDatabase _principalDatabase;

    @Override
    public void tearDown() throws Exception
    {
        if (_manager != null)
        {
            _manager.close();
        }
        super.tearDown();
    }

    private void setupMocks() throws Exception
    {
        _principalDatabase = mock(PrincipalDatabase.class);

        AuthenticationProviderInitialiser _mockMechInitialiser = mock(AuthenticationProviderInitialiser.class);
        Map<String, AuthenticationProviderInitialiser> _initialisers = Collections.singletonMap(MOCK_MECH_NAME, _mockMechInitialiser);

        when(_principalDatabase.getMechanisms()).thenReturn(_initialisers);

        _manager = new PrincipalDatabaseAuthenticationManager(_principalDatabase);
        _manager.initialise();
    }

    private void setupMocksWithInitialiser() throws Exception
    {
        _principalDatabase = mock(PrincipalDatabase.class);

        UsernamePasswordInitialiser usernamePasswordInitialiser = new UsernamePasswordInitialiser()
        {
            @Override
            public Class<? extends SaslServerFactory> getServerFactoryClassForJCARegistration()
            {
                return MySaslServerFactory.class;
            }

            @Override
            public String getMechanismName()
            {
                return MOCK_MECH_NAME;
            }
        };

        Map<String,AuthenticationProviderInitialiser> initialisers = new HashMap<String, AuthenticationProviderInitialiser>();
        initialisers.put(MOCK_MECH_NAME, usernamePasswordInitialiser);

        when(_principalDatabase.getMechanisms()).thenReturn(initialisers);

        usernamePasswordInitialiser.initialise(_principalDatabase);

        _manager = new PrincipalDatabaseAuthenticationManager(_principalDatabase);
        _manager.initialise();
    }

    /**
     * Tests that the PDAM registers SASL mechanisms correctly with the runtime.
     */
    public void testRegisteredMechanisms() throws Exception
    {
        //Ensure we haven't registered anything yet (though this would really indicate a prior test failure!)
        Provider qpidProvider = Security.getProvider(AuthenticationManager.PROVIDER_NAME);
        assertNull(qpidProvider);

        setupMocksWithInitialiser();

        assertNotNull(_manager.getMechanisms());
        assertEquals(MOCK_MECH_NAME, _manager.getMechanisms());

        qpidProvider = Security.getProvider(AuthenticationManager.PROVIDER_NAME);
        assertNotNull(qpidProvider);
    }

    /**
     * Tests that the SASL factory method createSaslServer correctly
     * returns a non-null implementation.
     */
    public void testSaslMechanismCreation() throws Exception
    {
        setupMocksWithInitialiser();

        SaslServer server = _manager.createSaslServer(MOCK_MECH_NAME, "localhost", null);
        assertNotNull(server);
        // Merely tests the creation of the mechanism. Mechanisms themselves are tested
        // by their own tests.
    }

    /**
     * Tests that the authenticate method correctly interprets an
     * authentication success.
     *
     */
    public void testSaslAuthenticationSuccess() throws Exception
    {
        setupMocks();

        SaslServer testServer = createTestSaslServer(true, false);

        AuthenticationResult result = _manager.authenticate(testServer, "12345".getBytes());

        assertOnlyContainsWrapped(PRINCIPAL, result.getPrincipals());
        assertEquals(AuthenticationStatus.SUCCESS, result.getStatus());
    }

    /**
     *
     * Tests that the authenticate method correctly interprets an
     * authentication not complete.
     *
     */
    public void testSaslAuthenticationNotCompleted() throws Exception
    {
        setupMocks();

        SaslServer testServer = createTestSaslServer(false, false);

        AuthenticationResult result = _manager.authenticate(testServer, "12345".getBytes());
        assertEquals("Principals was not expected size", 0, result.getPrincipals().size());

        assertEquals(AuthenticationStatus.CONTINUE, result.getStatus());
    }

    /**
     *
     * Tests that the authenticate method correctly interprets an
     * authentication error.
     *
     */
    public void testSaslAuthenticationError() throws Exception
    {
        setupMocks();

        SaslServer testServer = createTestSaslServer(false, true);

        AuthenticationResult result = _manager.authenticate(testServer, "12345".getBytes());
        assertEquals("Principals was not expected size", 0, result.getPrincipals().size());
        assertEquals(AuthenticationStatus.ERROR, result.getStatus());
    }

    public void testNonSaslAuthenticationSuccess() throws Exception
    {
        setupMocks();

        when(_principalDatabase.verifyPassword("guest", "guest".toCharArray())).thenReturn(true);

        AuthenticationResult result = _manager.authenticate("guest", "guest");
        assertOnlyContainsWrapped(PRINCIPAL, result.getPrincipals());
        assertEquals(AuthenticationStatus.SUCCESS, result.getStatus());
    }

    public void testNonSaslAuthenticationNotCompleted() throws Exception
    {
        setupMocks();

        when(_principalDatabase.verifyPassword("guest", "wrongpassword".toCharArray())).thenReturn(false);

        AuthenticationResult result = _manager.authenticate("guest", "wrongpassword");
        assertEquals("Principals was not expected size", 0, result.getPrincipals().size());
        assertEquals(AuthenticationStatus.CONTINUE, result.getStatus());
    }

    /**
     * Tests the ability to de-register the provider.
     */
    public void testClose() throws Exception
    {
        setupMocksWithInitialiser();

        assertEquals(MOCK_MECH_NAME, _manager.getMechanisms());
        assertNotNull(Security.getProvider(AuthenticationManager.PROVIDER_NAME));

        _manager.close();

        // Check provider has been removed.
        assertNull(_manager.getMechanisms());
        assertNull(Security.getProvider(AuthenticationManager.PROVIDER_NAME));
        _manager = null;
    }

    /**
     * Test SASL implementation used to test the authenticate() method.
     */
    private SaslServer createTestSaslServer(final boolean complete, final boolean throwSaslException)
    {
        return new MySaslServer(throwSaslException, complete);
    }

    public static final class MySaslServer implements SaslServer
    {
        private final boolean _throwSaslException;
        private final boolean _complete;

        public MySaslServer()
        {
            this(false, true);
        }

        private MySaslServer(boolean throwSaslException, boolean complete)
        {
            _throwSaslException = throwSaslException;
            _complete = complete;
        }

        public String getMechanismName()
        {
            return null;
        }

        public byte[] evaluateResponse(byte[] response) throws SaslException
        {
            if (_throwSaslException)
            {
                throw new SaslException("Mocked exception");
            }
            return null;
        }

        public boolean isComplete()
        {
            return _complete;
        }

        public String getAuthorizationID()
        {
            return _complete ? "guest" : null;
        }

        public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException
        {
            return null;
        }

        public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException
        {
            return null;
        }

        public Object getNegotiatedProperty(String propName)
        {
            return null;
        }

        public void dispose() throws SaslException
        {
        }
    }

    public static class MySaslServerFactory implements SaslServerFactory
    {
        @Override
        public SaslServer createSaslServer(String mechanism, String protocol,
                String serverName, Map<String, ?> props, CallbackHandler cbh)
                throws SaslException
        {
            if (MOCK_MECH_NAME.equals(mechanism))
            {
                return new MySaslServer();
            }
            else
            {
                return null;
            }
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props)
        {
            return new String[]{MOCK_MECH_NAME};
        }
    }
}
