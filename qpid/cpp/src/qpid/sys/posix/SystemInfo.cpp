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
 *
 */

#include "qpid/log/Statement.h"
#include "qpid/sys/SystemInfo.h"
#include "qpid/sys/posix/check.h"
#include <set>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <sys/utsname.h>
#include <sys/types.h> // For FreeBSD
#include <sys/socket.h> // For FreeBSD
#include <netinet/in.h> // For FreeBSD
#include <ifaddrs.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <netdb.h>

#ifndef HOST_NAME_MAX
#  define HOST_NAME_MAX 256
#endif

using namespace std;

namespace qpid {
namespace sys {

long  SystemInfo::concurrency() {
#ifdef _SC_NPROCESSORS_ONLN    // Linux specific.
    return sysconf(_SC_NPROCESSORS_ONLN);
#else
    return -1;
#endif
}

bool SystemInfo::getLocalHostname (Address &address) {
    char name[HOST_NAME_MAX];
    if (::gethostname(name, sizeof(name)) != 0)
        return false;
    address.host = name;
    return true;
}

static const string LOOPBACK("127.0.0.1");
static const string TCP("tcp");

// Test IPv4 address for loopback
inline bool IN_IS_ADDR_LOOPBACK(const ::in_addr* a) {
    return ((ntohl(a->s_addr) & 0xff000000) == 0x7f000000);
}

inline bool isLoopback(const ::sockaddr* addr) {
    switch (addr->sa_family) {
        case AF_INET: return IN_IS_ADDR_LOOPBACK(&((const ::sockaddr_in*)(const void*)addr)->sin_addr);
        case AF_INET6: return IN6_IS_ADDR_LOOPBACK(&((const ::sockaddr_in6*)(const void*)addr)->sin6_addr);
        default: return false;
    }
}

void SystemInfo::getLocalIpAddresses (uint16_t port,
                                      std::vector<Address> &addrList) {
    ::ifaddrs* ifaddr = 0;
    QPID_POSIX_CHECK(::getifaddrs(&ifaddr));
    for (::ifaddrs* ifap = ifaddr; ifap != 0; ifap = ifap->ifa_next) {
        if (ifap->ifa_addr == 0) continue;
        if (isLoopback(ifap->ifa_addr)) continue;
        int family = ifap->ifa_addr->sa_family;
        switch (family) {
            case AF_INET6: {
                // Ignore link local addresses as:
                // * The scope id is illegal in URL syntax
                // * Clients won't be able to use a link local address
                //   without adding their own (potentially different) scope id
                sockaddr_in6* sa6 = (sockaddr_in6*)(ifap->ifa_addr);
                if (IN6_IS_ADDR_LINKLOCAL(&sa6->sin6_addr)) break;
                // Fallthrough
            }
            case AF_INET: {
              char dispName[NI_MAXHOST];
              int rc = ::getnameinfo(
                  ifap->ifa_addr,
                  (family == AF_INET)
                  ? sizeof(struct sockaddr_in)
                  : sizeof(struct sockaddr_in6),
                  dispName, sizeof(dispName),
                  0, 0, NI_NUMERICHOST);
              if (rc != 0) {
                  throw QPID_POSIX_ERROR(rc);
              }
              string addr(dispName);
              addrList.push_back(Address(TCP, addr, port));
              break;
          }
          default:
            continue;
        }
    }
    ::freeifaddrs(ifaddr);

    if (addrList.empty()) {
        addrList.push_back(Address(TCP, LOOPBACK, port));
    }
}

namespace {
struct AddrInfo {
    struct addrinfo* ptr;
    AddrInfo(const std::string& host) : ptr(0) {
        if (::getaddrinfo(host.c_str(), NULL, NULL, &ptr) != 0)
            ptr = 0;
    }
    ~AddrInfo() { if (ptr) ::freeaddrinfo(ptr); }
};
}

bool SystemInfo::isLocalHost(const std::string& host) {
    std::vector<Address> myAddrs;
    getLocalIpAddresses(0, myAddrs);
    std::set<string> localHosts;
    for (std::vector<Address>::const_iterator i = myAddrs.begin(); i != myAddrs.end(); ++i)
        localHosts.insert(i->host);
    // Resolve host
    AddrInfo ai(host);
    if (!ai.ptr) return false;
    for (struct addrinfo *res = ai.ptr; res != NULL; res = res->ai_next) {
        if (isLoopback(res->ai_addr)) return true;
        // Get string form of IP addr
        char addr[NI_MAXHOST] = "";
        int error = ::getnameinfo(res->ai_addr, res->ai_addrlen, addr, NI_MAXHOST, NULL, 0,
                                  NI_NUMERICHOST | NI_NUMERICSERV);
        if (error) return false;
        if (localHosts.find(addr) != localHosts.end()) return true;
    }
    return false;
}

void SystemInfo::getSystemId (std::string &osName,
                              std::string &nodeName,
                              std::string &release,
                              std::string &version,
                              std::string &machine)
{
    struct utsname _uname;
    if (uname (&_uname) == 0)
    {
        osName = _uname.sysname;
        nodeName = _uname.nodename;
        release = _uname.release;
        version = _uname.version;
        machine = _uname.machine;
    }
}

uint32_t SystemInfo::getProcessId()
{
    return (uint32_t) ::getpid();
}

uint32_t SystemInfo::getParentProcessId()
{
    return (uint32_t) ::getppid();
}

// Linux specific (Solaris has quite different stuff in /proc)
string SystemInfo::getProcessName()
{
    string value;

    ifstream input("/proc/self/status");
    if (input.good()) {
        while (!input.eof()) {
            string key;
            input >> key;
            if (key == "Name:") {
                input >> value;
                break;
            }
        }
        input.close();
    }

    return value;
}

}} // namespace qpid::sys
