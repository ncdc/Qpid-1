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

#include "qpid/sys/ProtocolFactory.h"

#include "qpid/Plugin.h"
#include "qpid/broker/Broker.h"
#include "qpid/log/Statement.h"
#include "qpid/sys/AsynchIOHandler.h"
#include "qpid/sys/ConnectionCodec.h"
#include "qpid/sys/Socket.h"
#include "qpid/sys/SocketAddress.h"
#include "qpid/sys/SystemInfo.h"
#include "qpid/sys/windows/SslAsynchIO.h"

#include <boost/bind.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <memory>

// security.h needs to see this to distinguish from kernel use.
#define SECURITY_WIN32
#include <security.h>
#include <Schnlsp.h>
#undef SECURITY_WIN32


namespace qpid {
namespace sys {

class Timer;

namespace windows {

struct SslServerOptions : qpid::Options
{
    std::string certStore;
    std::string certStoreLocation;
    std::string certName;
    uint16_t port;
    bool clientAuth;

    SslServerOptions() : qpid::Options("SSL Options"),
                         certStore("My"),
                         certStoreLocation("CurrentUser"),
                         certName("localhost"),
                         port(5671),
                         clientAuth(false)
    {
        qpid::Address me;
        if (qpid::sys::SystemInfo::getLocalHostname(me))
            certName = me.host;

        addOptions()
            ("ssl-cert-store", optValue(certStore, "NAME"), "Local store name from which to obtain certificate")
            ("ssl-cert-store-location", optValue(certStoreLocation, "NAME"),
             "Local store name location for certificates ( CurrentUser | LocalMachine | CurrentService )")
            ("ssl-cert-name", optValue(certName, "NAME"), "Name of the certificate to use")
            ("ssl-port", optValue(port, "PORT"), "Port on which to listen for SSL connections")
            ("ssl-require-client-authentication", optValue(clientAuth), 
             "Forces clients to authenticate in order to establish an SSL connection");
    }
};

class SslProtocolFactory : public qpid::sys::ProtocolFactory {
    boost::ptr_vector<Socket> listeners;
    boost::ptr_vector<AsynchAcceptor> acceptors;
    Timer& brokerTimer;
    uint32_t maxNegotiateTime;
    uint16_t listeningPort;
    const bool tcpNoDelay;
    std::string brokerHost;
    const bool clientAuthSelected;
    std::auto_ptr<qpid::sys::AsynchAcceptor> acceptor;
    ConnectFailedCallback connectFailedCallback;
    CredHandle credHandle;

  public:
    SslProtocolFactory(const SslServerOptions&, const std::string& host, const std::string& port,
                       int backlog, bool nodelay,
                       Timer& timer, uint32_t maxTime);
    ~SslProtocolFactory();
    void accept(sys::Poller::shared_ptr, sys::ConnectionCodec::Factory*);
    void connect(sys::Poller::shared_ptr, const std::string& host, const std::string& port,
                 sys::ConnectionCodec::Factory*,
                 ConnectFailedCallback failed);

    uint16_t getPort() const;
    bool supports(const std::string& capability);

  private:
    void connectFailed(const qpid::sys::Socket&,
                       int err,
                       const std::string& msg);
    void established(sys::Poller::shared_ptr,
                     const qpid::sys::Socket&,
                     sys::ConnectionCodec::Factory*,
                     bool isClient);
};

// Static instance to initialise plugin
static struct SslPlugin : public Plugin {
    SslServerOptions options;

    Options* getOptions() { return &options; }

    void earlyInitialize(Target&) {
    }
    
    void initialize(Target& target) {
        broker::Broker* broker = dynamic_cast<broker::Broker*>(&target);
        // Only provide to a Broker
        if (broker) {
            try {
                const broker::Broker::Options& opts = broker->getOptions();
                ProtocolFactory::shared_ptr protocol(new SslProtocolFactory(options,
                                                                            "", boost::lexical_cast<std::string>(options.port),
                                                                            opts.connectionBacklog, opts.tcpNoDelay,
                                                                            broker->getTimer(), opts.maxNegotiateTime));
                QPID_LOG(notice, "Listening for SSL connections on TCP port " << protocol->getPort());
                broker->registerProtocolFactory("ssl", protocol);
            } catch (const std::exception& e) {
                QPID_LOG(error, "Failed to initialise SSL listener: " << e.what());
            }
        }
    }
} sslPlugin;

SslProtocolFactory::SslProtocolFactory(const SslServerOptions& options,
                                       const std::string& host, const std::string& port,
                                       int backlog, bool nodelay,
                                       Timer& timer, uint32_t maxTime)
    : brokerTimer(timer),
      maxNegotiateTime(maxTime),
      tcpNoDelay(nodelay),
      clientAuthSelected(options.clientAuth) {

    // Make sure that certificate store is good before listening to sockets
    // to avoid having open and listening sockets when there is no cert store
    SecInvalidateHandle(&credHandle);

    // Get the certificate for this server.
    DWORD flags = 0;
    std::string certStoreLocation = options.certStoreLocation;
    std::transform(certStoreLocation.begin(), certStoreLocation.end(), certStoreLocation.begin(), ::tolower);
    if (certStoreLocation == "currentuser") {
        flags = CERT_SYSTEM_STORE_CURRENT_USER;
    } else if (certStoreLocation == "localmachine") {
        flags = CERT_SYSTEM_STORE_LOCAL_MACHINE;
    } else if (certStoreLocation == "currentservice") {
        flags = CERT_SYSTEM_STORE_CURRENT_SERVICE;
    } else {
        QPID_LOG(error, "Unrecognised SSL certificate store location: " << options.certStoreLocation
            << " - Using default location");
    }
    HCERTSTORE certStoreHandle;
    certStoreHandle = ::CertOpenStore(CERT_STORE_PROV_SYSTEM_A,
                                      X509_ASN_ENCODING,
                                      0,
                                      flags |
                                      CERT_STORE_READONLY_FLAG,
                                      options.certStore.c_str());
    if (!certStoreHandle)
        throw qpid::Exception(QPID_MSG("Opening store " << options.certStore << " " << qpid::sys::strError(GetLastError())));

    PCCERT_CONTEXT certContext;
    certContext = ::CertFindCertificateInStore(certStoreHandle,
                                               X509_ASN_ENCODING,
                                               0,
                                               CERT_FIND_SUBJECT_STR_A,
                                               options.certName.c_str(),
                                               NULL);
    if (certContext == NULL) {
        int err = ::GetLastError();
        ::CertCloseStore(certStoreHandle, 0);
        throw qpid::Exception(QPID_MSG("Locating certificate " << options.certName << " in store " << options.certStore << " " << qpid::sys::strError(GetLastError())));
        throw QPID_WINDOWS_ERROR(err);
    }

    SCHANNEL_CRED cred;
    memset(&cred, 0, sizeof(cred));
    cred.dwVersion = SCHANNEL_CRED_VERSION;
    cred.cCreds = 1;
    cred.paCred = &certContext;
    SECURITY_STATUS status = ::AcquireCredentialsHandle(NULL,
                                                        UNISP_NAME,
                                                        SECPKG_CRED_INBOUND,
                                                        NULL,
                                                        &cred,
                                                        NULL,
                                                        NULL,
                                                        &credHandle,
                                                        NULL);
    if (status != SEC_E_OK)
        throw QPID_WINDOWS_ERROR(status);
    ::CertFreeCertificateContext(certContext);
    ::CertCloseStore(certStoreHandle, 0);

    // Listen to socket(s)
    SocketAddress sa(host, port);

    // We must have at least one resolved address
    QPID_LOG(info, "SSL Listening to: " << sa.asString())
    Socket* s = new Socket;
    listeningPort = s->listen(sa, backlog);
    listeners.push_back(s);

    // Try any other resolved addresses
    while (sa.nextAddress()) {
        QPID_LOG(info, "SSL Listening to: " << sa.asString())
        Socket* s = new Socket;
        s->listen(sa, backlog);
        listeners.push_back(s);
    }
}

SslProtocolFactory::~SslProtocolFactory() {
    ::FreeCredentialsHandle(&credHandle);
}

void SslProtocolFactory::connectFailed(const qpid::sys::Socket&,
                                       int err,
                                       const std::string& msg) {
    if (connectFailedCallback)
        connectFailedCallback(err, msg);
}

void SslProtocolFactory::established(sys::Poller::shared_ptr poller,
                                     const qpid::sys::Socket& s,
                                     sys::ConnectionCodec::Factory* f,
                                     bool isClient) {
    sys::AsynchIOHandler* async = new sys::AsynchIOHandler(s.getFullAddress(), f);

    if (tcpNoDelay) {
        s.setTcpNoDelay();
        QPID_LOG(info,
                 "Set TCP_NODELAY on connection to " << s.getPeerAddress());
    }

    SslAsynchIO *aio;
    if (isClient) {
        async->setClient();
        aio =
          new qpid::sys::windows::ClientSslAsynchIO(brokerHost,
                                                    s,
                                                    credHandle,
                                                    boost::bind(&AsynchIOHandler::readbuff, async, _1, _2),
                                                    boost::bind(&AsynchIOHandler::eof, async, _1),
                                                    boost::bind(&AsynchIOHandler::disconnect, async, _1),
                                                    boost::bind(&AsynchIOHandler::closedSocket, async, _1, _2),
                                                    boost::bind(&AsynchIOHandler::nobuffs, async, _1),
                                                    boost::bind(&AsynchIOHandler::idle, async, _1));
    }
    else {
        aio =
          new qpid::sys::windows::ServerSslAsynchIO(clientAuthSelected,
                                                    s,
                                                    credHandle,
                                                    boost::bind(&AsynchIOHandler::readbuff, async, _1, _2),
                                                    boost::bind(&AsynchIOHandler::eof, async, _1),
                                                    boost::bind(&AsynchIOHandler::disconnect, async, _1),
                                                    boost::bind(&AsynchIOHandler::closedSocket, async, _1, _2),
                                                    boost::bind(&AsynchIOHandler::nobuffs, async, _1),
                                                    boost::bind(&AsynchIOHandler::idle, async, _1));
    }

    async->init(aio, brokerTimer, maxNegotiateTime, 4);
    aio->start(poller);
}

uint16_t SslProtocolFactory::getPort() const {
    return listeningPort; // Immutable no need for lock.
}

void SslProtocolFactory::accept(sys::Poller::shared_ptr poller,
                                sys::ConnectionCodec::Factory* fact) {
    for (unsigned i = 0; i<listeners.size(); ++i) {
        acceptors.push_back(
            AsynchAcceptor::create(listeners[i],
                            boost::bind(&SslProtocolFactory::established, this, poller, _1, fact, false)));
        acceptors[i].start(poller);
    }
}

void SslProtocolFactory::connect(sys::Poller::shared_ptr poller,
                                 const std::string& host,
                                 const std::string& port,
                                 sys::ConnectionCodec::Factory* fact,
                                 ConnectFailedCallback failed)
{
    SCHANNEL_CRED cred;
    memset(&cred, 0, sizeof(cred));
    cred.dwVersion = SCHANNEL_CRED_VERSION;
    SECURITY_STATUS status = ::AcquireCredentialsHandle(NULL,
                                                        UNISP_NAME,
                                                        SECPKG_CRED_OUTBOUND,
                                                        NULL,
                                                        &cred,
                                                        NULL,
                                                        NULL,
                                                        &credHandle,
                                                        NULL);
    if (status != SEC_E_OK)
        throw QPID_WINDOWS_ERROR(status);

    brokerHost = host;
    // Note that the following logic does not cause a memory leak.
    // The allocated Socket is freed either by the AsynchConnector
    // upon connection failure or by the AsynchIO upon connection
    // shutdown.  The allocated AsynchConnector frees itself when it
    // is no longer needed.
    qpid::sys::Socket* socket = new qpid::sys::Socket();
    connectFailedCallback = failed;
    AsynchConnector::create(*socket,
                            host,
                            port,
                            boost::bind(&SslProtocolFactory::established,
                                        this, poller, _1, fact, true),
                            boost::bind(&SslProtocolFactory::connectFailed,
                                        this, _1, _2, _3));
}

namespace
{
const std::string SSL = "ssl";
}

bool SslProtocolFactory::supports(const std::string& capability)
{
    std::string s = capability;
    transform(s.begin(), s.end(), s.begin(), tolower);
    return s == SSL;
}

}}} // namespace qpid::sys::windows
