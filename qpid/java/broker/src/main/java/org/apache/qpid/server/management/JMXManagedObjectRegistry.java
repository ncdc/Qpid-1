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
package org.apache.qpid.server.management;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.Notification;
import javax.management.NotificationFilterSupport;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.MBeanServerForwarder;
import javax.management.remote.rmi.RMIConnection;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;
import javax.management.remote.rmi.RMIServerImpl;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;
import javax.security.auth.Subject;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.qpid.AMQException;
import org.apache.qpid.server.logging.actors.CurrentActor;
import org.apache.qpid.server.logging.messages.ManagementConsoleMessages;
import org.apache.qpid.server.registry.ApplicationRegistry;
import org.apache.qpid.server.registry.IApplicationRegistry;
import org.apache.qpid.server.security.auth.rmi.RMIPasswordAuthenticator;

/**
 * This class starts up an MBeanserver. If out of the box agent has been enabled then there are no
 * security features implemented like user authentication and authorisation.
 */
public class JMXManagedObjectRegistry implements ManagedObjectRegistry
{
    private static final Logger _log = Logger.getLogger(JMXManagedObjectRegistry.class);

    private final MBeanServer _mbeanServer;
    private JMXConnectorServer _cs;
    private Registry _rmiRegistry;
    private boolean _useCustomSocketFactory;

    private final int _jmxPortRegistryServer;
    private final int _jmxPortConnectorServer;

    public JMXManagedObjectRegistry() throws AMQException
    {
        _log.info("Initialising managed object registry using platform MBean server");
        IApplicationRegistry appRegistry = ApplicationRegistry.getInstance();

        // Retrieve the config parameters
        _useCustomSocketFactory = appRegistry.getConfiguration().getUseCustomRMISocketFactory();
        boolean platformServer = appRegistry.getConfiguration().getPlatformMbeanserver();

        _mbeanServer =
                platformServer ? ManagementFactory.getPlatformMBeanServer()
                : MBeanServerFactory.createMBeanServer(ManagedObject.DOMAIN);

        _jmxPortRegistryServer = appRegistry.getConfiguration().getJMXPortRegistryServer();
        _jmxPortConnectorServer = appRegistry.getConfiguration().getJMXConnectorServerPort();

    }

    public void start() throws IOException, ConfigurationException
    {

        CurrentActor.get().message(ManagementConsoleMessages.STARTUP());

        //check if system properties are set to use the JVM's out-of-the-box JMXAgent
        if (areOutOfTheBoxJMXOptionsSet())
        {
            CurrentActor.get().message(ManagementConsoleMessages.READY(true));
            return;
        }

        IApplicationRegistry appRegistry = ApplicationRegistry.getInstance();


        //Socket factories for the RMIConnectorServer, either default or SLL depending on configuration
        RMIClientSocketFactory csf;
        RMIServerSocketFactory ssf;

        //check ssl enabled option in config, default to true if option is not set
        boolean sslEnabled = appRegistry.getConfiguration().getManagementSSLEnabled();

        if (sslEnabled)
        {
            //set the SSL related system properties used by the SSL RMI socket factories to the values
            //given in the configuration file, unless command line settings have already been specified
            String keyStorePath;

            if(System.getProperty("javax.net.ssl.keyStore") != null)
            {
                keyStorePath = System.getProperty("javax.net.ssl.keyStore");
            }
            else
            {
                keyStorePath = appRegistry.getConfiguration().getManagementKeyStorePath();
            }

            //check the keystore path value is valid
            if (keyStorePath == null)
            {
                throw new ConfigurationException("JMX management SSL keystore path not defined, " +
                    		                     "unable to start SSL protected JMX ConnectorServer");
            }
            else
            {
                //ensure the system property is set
                System.setProperty("javax.net.ssl.keyStore", keyStorePath);

                //check the file is usable
                File ksf = new File(keyStorePath);

                if (!ksf.exists())
                {
                    throw new FileNotFoundException("Cannot find JMX management SSL keystore file: " + ksf);
                }
                if (!ksf.canRead())
                {
                    throw new FileNotFoundException("Cannot read JMX management SSL keystore file: "
                                                    + ksf +  ". Check permissions.");
                }

                CurrentActor.get().message(ManagementConsoleMessages.SSL_KEYSTORE(ksf.getAbsolutePath()));
            }

            //check the key store password is set
            if (System.getProperty("javax.net.ssl.keyStorePassword") == null)
            {

                if (appRegistry.getConfiguration().getManagementKeyStorePassword() == null)
                {
                    throw new ConfigurationException("JMX management SSL keystore password not defined, " +
                      		                         "unable to start requested SSL protected JMX server");
                }
                else
                {
                   System.setProperty("javax.net.ssl.keyStorePassword",
                           appRegistry.getConfiguration().getManagementKeyStorePassword());
                }
            }

            //create the SSL RMI socket factories
            csf = new SslRMIClientSocketFactory();
            ssf = new SslRMIServerSocketFactory();
        }
        else
        {
            //Do not specify any specific RMI socket factories, resulting in use of the defaults.
            csf = null;
            ssf = null;
        }

        //add a JMXAuthenticator implementation the env map to authenticate the RMI based JMX connector server
        RMIPasswordAuthenticator rmipa = new RMIPasswordAuthenticator();
        rmipa.setAuthenticationManager(appRegistry.getAuthenticationManager(new InetSocketAddress(_jmxPortRegistryServer)));
        HashMap<String,Object> env = new HashMap<String,Object>();
        env.put(JMXConnectorServer.AUTHENTICATOR, rmipa);

        /*
         * Start a RMI registry on the management port, to hold the JMX RMI ConnectorServer stub.
         * Using custom socket factory to prevent anyone (including us unfortunately) binding to the registry using RMI.
         * As a result, only binds made using the object reference will succeed, thus securing it from external change.
         */
        System.setProperty("java.rmi.server.randomIDs", "true");
        if(_useCustomSocketFactory)
        {
            _rmiRegistry = LocateRegistry.createRegistry(_jmxPortRegistryServer, null, new CustomRMIServerSocketFactory());
        }
        else
        {
            _rmiRegistry = LocateRegistry.createRegistry(_jmxPortRegistryServer, null, null);
        }

        CurrentActor.get().message(ManagementConsoleMessages.LISTENING("RMI Registry", _jmxPortRegistryServer));

        /*
         * We must now create the RMI ConnectorServer manually, as the JMX Factory methods use RMI calls
         * to bind the ConnectorServer to the registry, which will now fail as for security we have
         * locked it from any RMI based modifications, including our own. Instead, we will manually bind
         * the RMIConnectorServer stub to the registry using its object reference, which will still succeed.
         *
         * The registry is exported on the defined management port 'port'. We will export the RMIConnectorServer
         * on 'port +1'. Use of these two well-defined ports will ease any navigation through firewall's.
         */
        final Map<String, String> connectionIdUsernameMap = new ConcurrentHashMap<String, String>();
        final RMIServerImpl rmiConnectorServerStub = new RMIJRMPServerImpl(_jmxPortConnectorServer, csf, ssf, env)
        {

            /**
             * Override makeClient so we can cache the username of the client in a Map keyed by connectionId.
             * ConnectionId is guaranteed to be unique per client connection, according to the JMS spec.
             * An instance of NotificationListener (mapCleanupListener) will be responsible for removing these Map
             * entries.
             *
             * @see javax.management.remote.rmi.RMIJRMPServerImpl#makeClient(java.lang.String, javax.security.auth.Subject)
             */
            @Override
            protected RMIConnection makeClient(String connectionId, Subject subject) throws IOException
            {
                final RMIConnection makeClient = super.makeClient(connectionId, subject);
                final Principal principal = subject.getPrincipals().iterator().next();
                connectionIdUsernameMap.put(connectionId, principal.getName());
                return makeClient;
            }
        };

        // Create a Listener responsible for removing the map entries add by the #makeClient entry above.
        final NotificationListener mapCleanupListener = new NotificationListener()
        {

            @Override
            public void handleNotification(Notification notification, Object handback)
            {
                final String connectionId = ((JMXConnectionNotification) notification).getConnectionId();
                connectionIdUsernameMap.remove(connectionId);
            }
        };

        String localHost;
        try
        {
            localHost = InetAddress.getLocalHost().getHostName();
        }
        catch(UnknownHostException ex)
        {
            localHost="127.0.0.1";
        }
        final String hostname = localHost;
        final JMXServiceURL externalUrl = new JMXServiceURL(
                "service:jmx:rmi://"+hostname+":"+(_jmxPortConnectorServer)+"/jndi/rmi://"+hostname+":"+_jmxPortRegistryServer+"/jmxrmi");

        final JMXServiceURL internalUrl = new JMXServiceURL("rmi", hostname, _jmxPortConnectorServer);
        _cs = new RMIConnectorServer(internalUrl, env, rmiConnectorServerStub, _mbeanServer)
        {
            @Override
            public synchronized void start() throws IOException
            {
                try
                {
                    //manually bind the connector server to the registry at key 'jmxrmi', like the out-of-the-box agent
                    _rmiRegistry.bind("jmxrmi", rmiConnectorServerStub);
                }
                catch (AlreadyBoundException abe)
                {
                    //key was already in use. shouldnt happen here as its a new registry, unbindable by normal means.

                    //IOExceptions are the only checked type throwable by the method, wrap and rethrow
                    IOException ioe = new IOException(abe.getMessage());
                    ioe.initCause(abe);
                    throw ioe;
                }

                //now do the normal tasks
                super.start();
            }

            @Override
            public synchronized void stop() throws IOException
            {
                try
                {
                    if (_rmiRegistry != null)
                    {
                        _rmiRegistry.unbind("jmxrmi");
                    }
                }
                catch (NotBoundException nbe)
                {
                    //ignore
                }

                //now do the normal tasks
                super.stop();
            }

            @Override
            public JMXServiceURL getAddress()
            {
                //must return our pre-crafted url that includes the full details, inc JNDI details
                return externalUrl;
            }

        };


        //Add the custom invoker as an MBeanServerForwarder, and start the RMIConnectorServer.
        MBeanServerForwarder mbsf = MBeanInvocationHandlerImpl.newProxyInstance();
        _cs.setMBeanServerForwarder(mbsf);


        // Get the handler that is used by the above MBInvocationHandler Proxy.
        // which is the MBeanInvocationHandlerImpl and so also a NotificationListener.
        final NotificationListener invocationHandler = (NotificationListener) Proxy.getInvocationHandler(mbsf);

        // Install a notification listener on OPENED, CLOSED, and FAILED,
        // passing the map of connection-ids to usernames as hand-back data.
        final NotificationFilterSupport invocationHandlerFilter = new NotificationFilterSupport();
        invocationHandlerFilter.enableType(JMXConnectionNotification.OPENED);
        invocationHandlerFilter.enableType(JMXConnectionNotification.CLOSED);
        invocationHandlerFilter.enableType(JMXConnectionNotification.FAILED);
        _cs.addNotificationListener(invocationHandler, invocationHandlerFilter, connectionIdUsernameMap);

        // Install a second notification listener on CLOSED AND FAILED only to remove the entry from the
        // Map.  Here we rely on the fact that JMX will call the listeners in the order in which they are
        // installed.
        final NotificationFilterSupport mapCleanupHandlerFilter = new NotificationFilterSupport();
        mapCleanupHandlerFilter.enableType(JMXConnectionNotification.CLOSED);
        mapCleanupHandlerFilter.enableType(JMXConnectionNotification.FAILED);
        _cs.addNotificationListener(mapCleanupListener, mapCleanupHandlerFilter, null);

        _cs.start();

        String connectorServer = (sslEnabled ? "SSL " : "") + "JMX RMIConnectorServer";
        CurrentActor.get().message(ManagementConsoleMessages.LISTENING(connectorServer, _jmxPortConnectorServer));

        CurrentActor.get().message(ManagementConsoleMessages.READY(false));
    }

    /*
     * Custom RMIServerSocketFactory class, used to prevent updates to the RMI registry.
     * Supplied to the registry at creation, this will prevent RMI-based operations on the
     * registry such as attempting to bind a new object, thereby securing it from tampering.
     * This is accomplished by always returning null when attempting to determine the address
     * of the caller, thus ensuring the registry will refuse the attempt. Calls to bind etc
     * made using the object reference will not be affected and continue to operate normally.
     */

    private static class CustomRMIServerSocketFactory implements RMIServerSocketFactory
    {

        public ServerSocket createServerSocket(int port) throws IOException
        {
            return new NoLocalAddressServerSocket(port);
        }

        private static class NoLocalAddressServerSocket extends ServerSocket
        {
            NoLocalAddressServerSocket(int port) throws IOException
            {
                super(port);
            }

            @Override
            public Socket accept() throws IOException
            {
                Socket s = new NoLocalAddressSocket();
                super.implAccept(s);
                return s;
            }
        }

        private static class NoLocalAddressSocket extends Socket
        {
            @Override
            public InetAddress getInetAddress()
            {
                return null;
            }
        }
    }


    public void registerObject(ManagedObject managedObject) throws JMException
    {
        _mbeanServer.registerMBean(managedObject, managedObject.getObjectName());
    }

    public void unregisterObject(ManagedObject managedObject) throws JMException
    {
        _mbeanServer.unregisterMBean(managedObject.getObjectName());
    }

    // checks if the system properties are set which enable the JVM's out-of-the-box JMXAgent.
    private boolean areOutOfTheBoxJMXOptionsSet()
    {
        if (System.getProperty("com.sun.management.jmxremote") != null)
        {
            return true;
        }

        if (System.getProperty("com.sun.management.jmxremote.port") != null)
        {
            return true;
        }

        return false;
    }

    //Stops the JMXConnectorServer and RMIRegistry, then unregisters any remaining MBeans from the MBeanServer
    public void close()
    {
        if (_cs != null)
        {
            // Stopping the JMX ConnectorServer
            try
            {
                CurrentActor.get().message(ManagementConsoleMessages.SHUTTING_DOWN("JMX RMIConnectorServer", _cs.getAddress().getPort()));
                _cs.stop();
            }
            catch (IOException e)
            {
                _log.error("Exception while closing the JMX ConnectorServer: " + e.getMessage());
            }
        }

        if (_rmiRegistry != null)
        {
            // Stopping the RMI registry
            CurrentActor.get().message(ManagementConsoleMessages.SHUTTING_DOWN("RMI Registry", _jmxPortRegistryServer));
            try
            {
                UnicastRemoteObject.unexportObject(_rmiRegistry, false);
            }
            catch (NoSuchObjectException e)
            {
                _log.error("Exception while closing the RMI Registry: " + e.getMessage());
            }
        }

        //ObjectName query to gather all Qpid related MBeans
        ObjectName mbeanNameQuery = null;
        try
        {
            mbeanNameQuery = new ObjectName(ManagedObject.DOMAIN + ":*");
        }
        catch (Exception e1)
        {
            _log.warn("Unable to generate MBean ObjectName query for close operation");
        }

        for (ObjectName name : _mbeanServer.queryNames(mbeanNameQuery, null))
        {
            try
            {
                _mbeanServer.unregisterMBean(name);
            }
            catch (JMException e)
            {
                _log.error("Exception unregistering MBean '"+ name +"': " + e.getMessage());
            }
        }

        CurrentActor.get().message(ManagementConsoleMessages.STOPPED());
    }

}
