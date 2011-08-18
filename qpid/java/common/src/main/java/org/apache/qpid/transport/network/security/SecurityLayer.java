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
package org.apache.qpid.transport.network.security;

import java.nio.ByteBuffer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.qpid.ssl.SSLContextFactory;
import org.apache.qpid.transport.Connection;
import org.apache.qpid.transport.ConnectionListener;
import org.apache.qpid.transport.ConnectionSettings;
import org.apache.qpid.transport.Receiver;
import org.apache.qpid.transport.Sender;
import org.apache.qpid.transport.TransportException;
import org.apache.qpid.transport.network.security.sasl.SASLReceiver;
import org.apache.qpid.transport.network.security.sasl.SASLSender;
import org.apache.qpid.transport.network.security.ssl.SSLReceiver;
import org.apache.qpid.transport.network.security.ssl.SSLSender;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

public class SecurityLayer
{
    ConnectionSettings settings;
    Connection con;
    SSLSecurityLayer sslLayer;
    SASLSecurityLayer saslLayer;

    public SecurityLayer(Connection con)
    {
        this.con = con;
        this.settings = con.getConnectionSettings();
        if (settings.isUseSSL())
        {
            sslLayer = new SSLSecurityLayer();
        }
        if (settings.isUseSASLEncryption())
        {
            saslLayer = new SASLSecurityLayer();
        }
    }

    public Sender<ByteBuffer> sender(Sender<ByteBuffer> delegate)
    {
        Sender<ByteBuffer> sender = delegate;
        
        if (settings.isUseSSL())
        {
            sender = sslLayer.sender(sender);
        }     
        
        if (settings.isUseSASLEncryption())
        {
            sender = saslLayer.sender(sender);
        }
        
        return sender;
    }
    
    public Receiver<ByteBuffer> receiver(Receiver<ByteBuffer> delegate)
    {
        Receiver<ByteBuffer> receiver = delegate;
        
        if (settings.isUseSSL())
        {
            receiver = sslLayer.receiver(receiver);
        }        
        
        if (settings.isUseSASLEncryption())
        {
            receiver = saslLayer.receiver(receiver);
        }
        
        return receiver;
    }
    
    public String getUserID()
    {
        if (settings.isUseSSL())
        {
            return sslLayer.getUserID();
        }
        else
        {
            return null;
        }
    }
    
    class SSLSecurityLayer
    {
        final SSLEngine _engine;
        final SSLStatus _sslStatus = new SSLStatus();
                
        public SSLSecurityLayer() 
        {
            SSLContext sslCtx;
            try
            {
                sslCtx = SSLContextFactory
                        .buildClientContext(settings.getTrustStorePath(),
                                settings.getTrustStorePassword(),
                                settings.getTrustStoreCertType(),
                                settings.getKeyStorePath(),
                                settings.getKeyStorePassword(),
                                settings.getKeyStoreCertType(),
                                settings.getCertAlias());
            }
            catch (Exception e)
            {
                throw new TransportException("Error creating SSL Context", e);
            }
            
            try
            {
                _engine = sslCtx.createSSLEngine();
                _engine.setUseClientMode(true);
            }
            catch(Exception e)
            {
                throw new TransportException("Error creating SSL Engine", e);
            }
        }
        
        public SSLSender sender(Sender<ByteBuffer> delegate)
        {
            SSLSender sender = new SSLSender(_engine, delegate, _sslStatus);
            sender.setConnectionSettings(settings);
            return sender;
        }
        
        public SSLReceiver receiver(Receiver<ByteBuffer> delegate)
        {
            SSLReceiver receiver = new SSLReceiver(_engine, delegate, _sslStatus);
            receiver.setConnectionSettings(settings);
            return receiver;
        }
        
        public String getUserID()
        {
            return SSLUtil.retriveIdentity(_engine);
        }
        
    }
    
    class SASLSecurityLayer
    {
        public SASLSecurityLayer() 
        {
        }
        
        public SASLSender sender(Sender<ByteBuffer> delegate)
        {
            SASLSender sender = new SASLSender(delegate);
            con.addConnectionListener((ConnectionListener)sender);
            return sender;
        }
        
        public SASLReceiver receiver(Receiver<ByteBuffer> delegate)
        {
            SASLReceiver receiver = new SASLReceiver(delegate);
            con.addConnectionListener((ConnectionListener)receiver);
            return receiver;
        }
        
    }
}
