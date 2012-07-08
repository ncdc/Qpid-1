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
package org.apache.qpid.server.model.adapter;

import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.qpid.common.QpidProperties;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.Statistics;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.registry.IApplicationRegistry;
import org.apache.qpid.server.security.auth.manager.AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.IAuthenticationManagerRegistry;
import org.apache.qpid.server.transport.QpidAcceptor;
import org.apache.qpid.server.virtualhost.VirtualHostRegistry;

public class BrokerAdapter extends AbstractAdapter implements Broker, VirtualHostRegistry.RegistryChangeListener,
                                                              IApplicationRegistry.PortBindingListener,
                                                              IAuthenticationManagerRegistry.RegistryChangeListener
{


    private final IApplicationRegistry _applicationRegistry;
    private String _name;
    private final Map<org.apache.qpid.server.virtualhost.VirtualHost, VirtualHostAdapter> _vhostAdapters =
            new HashMap<org.apache.qpid.server.virtualhost.VirtualHost, VirtualHostAdapter>();
    private final StatisticsAdapter _statistics;
    private final Map<QpidAcceptor, PortAdapter> _portAdapters = new HashMap<QpidAcceptor, PortAdapter>();
    private HTTPPortAdapter _httpManagementPort;
    private final Map<AuthenticationManager, AuthenticationProviderAdapter> _authManagerAdapters =
            new HashMap<AuthenticationManager, AuthenticationProviderAdapter>();


    public BrokerAdapter(final IApplicationRegistry instance)
    {
        super(UUIDGenerator.generateRandomUUID());
        _applicationRegistry = instance;
        _name = "Broker";
        _statistics = new StatisticsAdapter(instance);

        instance.getVirtualHostRegistry().addRegistryChangeListener(this);
        populateVhosts();
        instance.addPortBindingListener(this);
        populatePorts();
        instance.addRegistryChangeListener(this);
        populateAuthenticationManagers();
    }

    private void populateVhosts()
    {
        synchronized(_vhostAdapters)
        {
            Collection<org.apache.qpid.server.virtualhost.VirtualHost> actualVhosts =
                    _applicationRegistry.getVirtualHostRegistry().getVirtualHosts();
            for(org.apache.qpid.server.virtualhost.VirtualHost vh : actualVhosts)
            {
                if(!_vhostAdapters.containsKey(vh))
                {
                    _vhostAdapters.put(vh, new VirtualHostAdapter(this, vh));
                }
            }

        }
    }


    public Collection<VirtualHost> getVirtualHosts()
    {
        synchronized(_vhostAdapters)
        {
            return new ArrayList<VirtualHost>(_vhostAdapters.values());
        }

    }
    private void populatePorts()
    {
        synchronized (_portAdapters)
        {
            Map<InetSocketAddress, QpidAcceptor> acceptors = _applicationRegistry.getAcceptors();

            for(Map.Entry<InetSocketAddress, QpidAcceptor> entry : acceptors.entrySet())
            {
                if(!_portAdapters.containsKey(entry.getValue()))
                {
                    _portAdapters.put(entry.getValue(), new PortAdapter(this, entry.getValue(), entry.getKey()));
                }
            }
            if(_applicationRegistry.useHTTPManagement())
            {
                _httpManagementPort = new HTTPPortAdapter(this, _applicationRegistry.getHTTPManagementPort());
            }

        }
    }

    public Collection<Port> getPorts()
    {
        synchronized (_portAdapters)
        {
            final ArrayList<Port> ports = new ArrayList<Port>(_portAdapters.values());
            if(_httpManagementPort != null)
            {
                ports.add(_httpManagementPort);
            }
            return ports;
        }
    }

    private void populateAuthenticationManagers()
    {
        synchronized (_authManagerAdapters)
        {
            IAuthenticationManagerRegistry authenticationManagerRegistry =
                    _applicationRegistry.getAuthenticationManagerRegistry();
            if(authenticationManagerRegistry != null)
            {
                Map<String, AuthenticationManager> authenticationManagers =
                        authenticationManagerRegistry.getAvailableAuthenticationManagers();

                for(Map.Entry<String, AuthenticationManager> entry : authenticationManagers.entrySet())
                {
                    if(!_authManagerAdapters.containsKey(entry.getValue()))
                    {
                        _authManagerAdapters.put(entry.getValue(),
                                                 AuthenticationProviderAdapter.createAuthenticationProviderAdapter(this,
                                                                                                                   entry.getValue()));
                    }
                }
            }
        }
    }

    public Collection<AuthenticationProvider> getAuthenticationProviders()
    {
        synchronized (_authManagerAdapters)
        {
            final ArrayList<AuthenticationProvider> authManagers =
                    new ArrayList<AuthenticationProvider>(_authManagerAdapters.values());
            return authManagers;
        }

    }

    public VirtualHost createVirtualHost(final String name,
                                         final State initialState,
                                         final boolean durable,
                                         final LifetimePolicy lifetime,
                                         final long ttl,
                                         final Map<String, Object> attributes)
            throws AccessControlException, IllegalArgumentException
    {
        return null;  //TODO
    }

    public VirtualHost createVirtualHost(final Map<String, Object> attributes)
            throws AccessControlException, IllegalArgumentException
    {
        return null;  //TODO
    }

    public void deleteVirtualHost(final VirtualHost vhost)
        throws AccessControlException, IllegalStateException
    {
        //TODO
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public String getName()
    {
        return _name;
    }

    public String setName(final String currentName, final String desiredName)
            throws IllegalStateException, AccessControlException
    {
        return null;  //TODO
    }


    public State getActualState()
    {
        return null;  //TODO
    }


    public boolean isDurable()
    {
        return true;
    }

    public void setDurable(final boolean durable)
            throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        throw new IllegalStateException();
    }

    public LifetimePolicy getLifetimePolicy()
    {
        return LifetimePolicy.PERMANENT;
    }

    public LifetimePolicy setLifetimePolicy(final LifetimePolicy expected, final LifetimePolicy desired)
            throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        throw new IllegalStateException();
    }

    public long getTimeToLive()
    {
        return 0;
    }

    public long setTimeToLive(final long expected, final long desired)
            throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        throw new IllegalStateException();
    }

    public Statistics getStatistics()
    {
        return _statistics;
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(Class<C> clazz)
    {
        if(clazz == VirtualHost.class)
        {
            return (Collection<C>) getVirtualHosts();
        }
        else if(clazz == Port.class)
        {
            return (Collection<C>) getPorts();
        }
        else if(clazz == AuthenticationProvider.class)
        {
            return (Collection<C>) getAuthenticationProviders();
        }

        return Collections.emptySet();
    }

    @Override
    public <C extends ConfiguredObject> C createChild(Class<C> childClass, Map<String, Object> attributes, ConfiguredObject... otherParents)
    {
        if(childClass == VirtualHost.class)
        {
            return (C) createVirtualHost(attributes);
        }
        else if(childClass == Port.class)
        {
            return (C) createPort(attributes);
        }
        else if(childClass == AuthenticationProvider.class)
        {
            return (C) createAuthenticationProvider(attributes);
        }
        else
        {
            throw new IllegalArgumentException("Cannot create child of class " + childClass.getSimpleName());
        }
    }

    private Port createPort(Map<String, Object> attributes)
    {
        // TODO
        return null;
    }

    private AuthenticationProvider createAuthenticationProvider(Map<String,Object> attributes)
    {
        // TODO
        return null;
    }


    public void virtualHostRegistered(org.apache.qpid.server.virtualhost.VirtualHost virtualHost)
    {
        VirtualHostAdapter adapter = null;
        synchronized (_vhostAdapters)
        {
            if(!_vhostAdapters.containsKey(virtualHost))
            {
                adapter = new VirtualHostAdapter(this, virtualHost);
                _vhostAdapters.put(virtualHost, adapter);
            }
        }
        if(adapter != null)
        {
            childAdded(adapter);
        }
    }

    public void virtualHostUnregistered(org.apache.qpid.server.virtualhost.VirtualHost virtualHost)
    {
        VirtualHostAdapter adapter = null;

        synchronized (_vhostAdapters)
        {
            adapter = _vhostAdapters.remove(virtualHost);
        }
        if(adapter != null)
        {
            childRemoved(adapter);
        }
    }

    @Override
    public void authenticationManagerRegistered(AuthenticationManager authenticationManager)
    {
        AuthenticationProviderAdapter adapter = null;
        synchronized (_authManagerAdapters)
        {
            if(!_authManagerAdapters.containsKey(authenticationManager))
            {
                adapter =
                        AuthenticationProviderAdapter.createAuthenticationProviderAdapter(this, authenticationManager);
                _authManagerAdapters.put(authenticationManager, adapter);
            }
        }
        if(adapter != null)
        {
            childAdded(adapter);
        }
    }

    @Override
    public void authenticationManagerUnregistered(AuthenticationManager authenticationManager)
    {
        AuthenticationProviderAdapter adapter;
        synchronized (_authManagerAdapters)
        {
            adapter = _authManagerAdapters.remove(authenticationManager);
        }
        if(adapter != null)
        {
            childRemoved(adapter);
        }
    }


    @Override
    public void bound(QpidAcceptor acceptor, InetSocketAddress bindAddress)
    {
        synchronized (_portAdapters)
        {
            if(!_portAdapters.containsKey(acceptor))
            {
                PortAdapter adapter = new PortAdapter(this, acceptor, bindAddress);
                _portAdapters.put(acceptor, adapter);
                childAdded(adapter);
            }
        }
    }

    @Override
    public void unbound(QpidAcceptor acceptor)
    {
        PortAdapter adapter = null;

        synchronized (_portAdapters)
        {
            adapter = _portAdapters.remove(acceptor);
        }
        if(adapter != null)
        {
            childRemoved(adapter);
        }
    }

    @Override
    public Collection<String> getAttributeNames()
    {
        return AVAILABLE_ATTRIBUTES;
    }

    @Override
    public Object getAttribute(String name)
    {
        if(ID.equals(name))
        {
            return getId();
        }
        else if(NAME.equals(name))
        {
            return getName();
        }
        else if(STATE.equals(name))
        {
            return State.ACTIVE;
        }
        else if(DURABLE.equals(name))
        {
            return isDurable();
        }
        else if(LIFETIME_POLICY.equals(name))
        {
            return LifetimePolicy.PERMANENT;
        }
        else if(TIME_TO_LIVE.equals(name))
        {
            // TODO
        }
        else if(CREATED.equals(name))
        {
            // TODO
        }
        else if(UPDATED.equals(name))
        {
            // TODO
        }
        else if(BUILD_VERSION.equals(name))
        {
            return QpidProperties.getBuildVersion();
        }
        else if(BYTES_RETAINED.equals(name))
        {
            // TODO
        }
        else if(OPERATING_SYSTEM.equals(name))
        {
            return System.getProperty("os.name") + " "
                   + System.getProperty("os.version") + " "
                   + System.getProperty("os.arch");
        }
        else if(PLATFORM.equals(name))
        {
            return System.getProperty("java.vendor") + " "
                   + System.getProperty("java.runtime.version", System.getProperty("java.version"));
        }
        else if(PROCESS_PID.equals(name))
        {
            // TODO
        }
        else if(PRODUCT_VERSION.equals(name))
        {
            return QpidProperties.getReleaseVersion();
        }
        else if(SUPPORTED_STORE_TYPES.equals(name))
        {
            // TODO
        }

        return super.getAttribute(name);    //TODO - Implement.
    }

    @Override
    public Object setAttribute(String name, Object expected, Object desired)
            throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        return super.setAttribute(name, expected, desired);    //TODO - Implement.
    }
}
