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


// 
// This module provides the backend recovery driver for Windows resource managers based on
// the IDtcToXaHelperSinglePipe interface.  The dll is loaded (LoadLibrary) directly into DTC
// itself and runs at a different protection level from the resource manager instance, which
// runs inside the application.
//
// The DTC dynamically loads this file, calls GetXaSwitch() to access the XA interface
// implementation and unloads the dll when done.
//
// This DTC plugin is only called for registration and recovery.  Each time the application
// registers the Qpid resource manager with DTC, the plugin is loaded and a successful
// connection via xa_open is confirmed before completing registration and saving the DSN
// connection string in the DTC log for possible recovery.  On recovery, the DSN is re-used to
// re-establish a new connection with the broker and perform recovery.
//
// Because this plugin is not involved in coordinating any active transactions it only needs to
// partially implement the XA interface.
//
// For the same reason, the locking strategy is simple.  A single global lock is used.
// Whenever networking activity is about to take place, the lock is relinquished and retaken
// soon thereafter.


#include <windows.h>
#include <transact.h>
#include <xolehlp.h>
#include <txdtc.h>
#include <xa.h>

#include "qpid/client/AsyncSession.h"
#include "qpid/client/Connection.h"
#include "qpid/framing/FieldValue.h"


#include <map>
#include <iostream>
#include <fstream>

namespace Apache {
namespace Qpid {
namespace DtcPlugin {

using namespace qpid::client;
using namespace qpid::framing;
using namespace qpid::framing::dtx;

class ResourceManager
{
private:
    Connection qpidConnection;
    Session qpidSession;
    bool active;
    std::string host;
    int port;
    std::string username;
    std::string password;
    bool ssl;
    bool saslPlain;

    int rmid;
    std::vector<qpid::framing::Xid> inDoubtXids;
    // current scan position, or -1 if no scan
    int cursor;
public:
    ResourceManager(int id, std::string h, int p, bool sslP, bool saslPlainP, std::string uname, std::string pass)
	: rmid(id), host(h), port(p), ssl(sslP), saslPlain(saslPlainP), username(uname), password(pass), 
	  active(false), cursor(-1) {}
    ~ResourceManager() {}
    INT open();
    INT close();
    INT commit(XID *xid);
    INT rollback(XID *xid);
    INT recover(XID *xids, long count, long flags);
};


CRITICAL_SECTION rmLock;

std::map<int, ResourceManager*> rmMap;
HMODULE thisDll = NULL;
bool memLocked = false;

#define QPIDHMCHARS 512


void pinDll() {
    if (!memLocked) {
	char thisDllName[QPIDHMCHARS];
	HMODULE ignore;

	DWORD nc = GetModuleFileName(thisDll, thisDllName, QPIDHMCHARS);
	if ((nc > 0) && (nc < QPIDHMCHARS)) {
	    memLocked = GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_PIN, thisDllName, &ignore);
	}
    }
}


void XaToQpid(XID &winXid, Xid &qpidXid) {
    // convert from XA defined structure XID to the Qpid framing structure
    qpidXid.setFormat((uint32_t) winXid.formatID);
    int bqualPos = 0;
    if (winXid.gtrid_length > 0) {
	qpidXid.setGlobalId(std::string(winXid.data, winXid.gtrid_length));
	bqualPos = winXid.gtrid_length;
    }
    if (winXid.bqual_length > 0) {
	qpidXid.setBranchId(std::string(winXid.data + bqualPos, winXid.bqual_length));
    }
}


// this function assumes that the qpidXid has already been validated for the memory copy

void QpidToXa(Xid &qpidXid, XID &winXid) {
    // convert from the Qpid framing structure to the XA defined structure XID
    winXid.formatID = qpidXid.getFormat();

    const std::string& global_s = qpidXid.getGlobalId();
    size_t gl = global_s.size();
    winXid.gtrid_length = (long) gl;
    if (gl > 0)
	global_s.copy(winXid.data, gl);
    
    const std::string branch_s = qpidXid.getBranchId();
    size_t bl = branch_s.size();
    winXid.bqual_length = (long) bl;
    if (bl > 0)
	branch_s.copy(winXid.data + gl, bl);
}


static char *dsnHeader = "QPIDdsnV2";

const char* nextDot(const char *p) {
    while (*p && (*p != '.'))
	p++;
    return p;
}

int getHexChar (char c) {
    if ((c >= '0') && (c <= '9'))
	return c - '0';

    if ((c >= 'a') && (c <= 'f'))
	return 10 + (c - 'a');

    if ((c >= 'A') && (c <= 'F'))
	return 10 + (c - 'A');
    
    return -1;
}

bool parseFromHex(const char* start, const char* end, std::string& target)
{
    const char *p = start;

    while ((p + 1) < end) {
	int nibble = getHexChar(*p++);
	if (nibble < 0)
	    return false;
	int byte = (nibble << 4);
	nibble = getHexChar(*p++);
	if (nibble < 0)
	    return false;
	byte += nibble;
	target.append (1, (char) byte & 0xFF);
    }
    return (p == end);
}


// parse string from AmqpConnection::DataSourcename
//   "QPIDdsnV2.port.host.instance_id.SSL_tf.SASL_mech.username.password"
//
// parse strictly and return false if the dsn is in a bad format

bool parseDsn (const char *dsn, std::string& host, int& port, bool& ssl, bool& saslPlain,
	       std::string& username, std::string& password) {
    if (dsn == NULL)
	return false;

    size_t len = strnlen(dsn, 1025);
    if (len > 1024)
	return false;

    if (strncmp(dsn, dsnHeader, strlen(dsnHeader)))
	return false;

    const char *endp = dsn + len;
    const char *tokenp = dsn + strlen(dsnHeader);
    if (*tokenp != '.')
	return false;

    // port
    tokenp++;
    if (tokenp >= endp)
	return false;
    if (*tokenp == '.')
	return false;		// null port not allowed

    const char *token_end = nextDot(tokenp);
    if ((token_end - tokenp) > 5)
	return false;

    port = 0;
    for (const char *p = tokenp; p < token_end; p++) {
	if ((*p < '0') || (*p > '9'))
	    return false;
	port = (10 * port) + (*p - '0');
    }

    if (port > 65535)
	return false;

    // host
    tokenp = token_end + 1;
    if (tokenp >= endp)
	return false;
    if (*tokenp == '.')
	return false;		// null host not allowed

    token_end = nextDot(tokenp);
    if (!parseFromHex(tokenp, token_end, host))
	return false;

    // skip the RM identifier, but verify it exists
    tokenp = token_end + 1;
    if (tokenp >= endp)
	return false;
    token_end = nextDot (tokenp);
    if ((token_end - tokenp) < 3)
	return false;

    // ssl: look for T or F
    tokenp = token_end + 1;
    if (tokenp >= endp)
	return false;
    if (*tokenp == 'T')
	ssl = true;
    else if (*tokenp == 'F')
	ssl = false;
    else
	return false;
    if (*++tokenp != '.')
	return false;

    // sasl mechanism: A = anonymous, P = plain.  More to come...
    ++tokenp;
    if (tokenp >= endp)
	return false;
    if (*(tokenp+1) != '.')
	return false;

    if (*tokenp == 'A') {
	saslPlain = false;
	tokenp += 2;
	// no auth tokens
    }
    else if (*tokenp == 'P') {
	saslPlain = true;
	tokenp += 2;
	if (tokenp >= endp)
	    return false;
	token_end = nextDot (tokenp);
	if (!parseFromHex(tokenp, token_end, username))
	    return false;
	tokenp = token_end + 1;
	
	if (tokenp >= endp)
	    return false;
	token_end = nextDot (tokenp);
	if (!parseFromHex(tokenp, token_end, password))
	    return false;
	tokenp = token_end + 1;
    }
    else
	return false;

    return (tokenp == endp);
}



INT ResourceManager::open() {
    INT rv = XAER_RMERR;	// placeholder until we successfully connect to resource
    active = true;
    LeaveCriticalSection(&rmLock);

    try {
	ConnectionSettings settings;
	settings.host = this->host;
	settings.port = this->port;


	if (ssl)
	    settings.protocol = "ssl";

	if (saslPlain) {
	    settings.username = this->username;
	    settings.password = this->password;
	    settings.mechanism = "PLAIN";
	}

	qpidConnection.open(settings);
	qpidSession = qpidConnection.newSession();
	rv = XA_OK;
/*
TODO: logging
    } catch (const qpid::Exception& error) {
      // log it
    } catch (const std::exception& e2) {
      // log it
*/
    } catch (...) {
	// TODO: log it
    }
	
    EnterCriticalSection(&rmLock);
    active = false;
    return rv;
}


INT ResourceManager::close() {
    // should never be called when already sending other commands to broker
    if (active)
	return XAER_PROTO;

    INT rv = XAER_RMERR;	// placeholder until we successfully close resource
    active = true;
    LeaveCriticalSection(&rmLock);
    try {
	if (qpidSession.isValid()) {
	    qpidSession.close();
	}
	if (qpidConnection.isOpen()) {
	    qpidConnection.close();
	}
    } catch (...) {
	// TODO: log it
    }

    EnterCriticalSection(&rmLock);
    active = false;

    if (!qpidConnection.isOpen()) {
	rv = XA_OK;
    }
    return rv;
}


INT ResourceManager::commit(XID *xid) {
    if (active)
	return XAER_PROTO;

    INT rv = XAER_RMFAIL;
    active = true;
    LeaveCriticalSection(&rmLock);

    try {
	qpid::framing::Xid qpidXid;
	XaToQpid(*xid, qpidXid);

	XaResult xaResult = qpidSession.dtxCommit(qpidXid, false, true);
	if (xaResult.hasStatus()) {
	    uint16_t status = xaResult.getStatus();
	    switch ((XaStatus) status) {
	    case XA_STATUS_XA_OK:
	    case XA_STATUS_XA_RDONLY:
	    case XA_STATUS_XA_HEURCOM:
		rv = XA_OK;
		break;
		
	    default:
		// commit failed and a retry won't fix
		rv = XAER_RMERR;
		break;
	    }
	
	}
    } catch (...) {
	// TODO: log it
    }

    EnterCriticalSection(&rmLock);
    active = false;
    return rv;
}


INT ResourceManager::rollback(XID *xid) {
    if (active)
	return XAER_PROTO;

    INT rv = XAER_RMFAIL;
    active = true;
    LeaveCriticalSection(&rmLock);

    try {
	qpid::framing::Xid qpidXid;
	XaToQpid(*xid, qpidXid);

	XaResult xaResult = qpidSession.dtxRollback(qpidXid, true);
	if (xaResult.hasStatus()) {
	    uint16_t status = xaResult.getStatus();
	    switch ((XaStatus) status) {
	    case XA_STATUS_XA_OK:
	    case XA_STATUS_XA_HEURRB:
		rv = XA_OK;
		break;

	    default:
		// RM internal error
		rv = XA_RBPROTO;
		break;
	    }
	}
    } catch (...) {
	// TODO: log it
    }

    EnterCriticalSection(&rmLock);
    active = false;
    return rv;
}


INT ResourceManager::recover(XID *xids, long count, long flags) {
    if (active)
	return XAER_PROTO;

    if ((xids == NULL) && (count != 0))
	return XAER_INVAL;

    if (count < 0)
	return XAER_INVAL;

    if (!(flags & TMSTARTRSCAN) && (cursor == -1))
	// no existing scan and no scan requested
	return XAER_INVAL;

    INT status = XA_OK;

    if (flags & TMSTARTRSCAN) {
	// start a fresh scan
	cursor = -1;
	inDoubtXids.clear();
	active = true;
	LeaveCriticalSection(&rmLock);

	try {
	    // status if we can't talk to the broker
	    status = XAER_RMFAIL;

	    DtxRecoverResult dtxrr = qpidSession.dtxRecover(true);

	    // status if we can't process the xids
	    status = XAER_RMERR;

        std::vector<std::string> wireFormatXids(dtxrr.getInDoubt().size());
        std::transform(dtxrr.getInDoubt().begin(), dtxrr.getInDoubt().end(), wireFormatXids.begin(), Array::get<std::string, Array::ValuePtr>);

	    size_t nXids = wireFormatXids.size();

	    if (nXids > 0) {
		StructHelper decoder;
		Xid qpidXid;
		for (size_t i = 0; i < nXids; i++) {
		    decoder.decode (qpidXid, wireFormatXids[i]);
		    inDoubtXids.push_back(qpidXid);
		}
		
		// if we got here the decoder validated the Xids
		status = XA_OK;

		// make sure none are too big, just in case
		
		for (size_t i = 0; i < nXids; i++) {
		    Xid& xid = inDoubtXids[i];
		    size_t l1 = xid.hasGlobalId() ? xid.getGlobalId().size() : 0;
		    size_t l2 = xid.hasBranchId() ? xid.getBranchId().size() : 0;
		    if ((l1 > MAXGTRIDSIZE) || (l2 > MAXBQUALSIZE) ||
			((l1 + l2) > XIDDATASIZE)) {
			status = XAER_RMERR;
			break;
		    }
		}
	    }
	    else {
		// nXids == 0, the previously cleared inDoubtXids is correctly populated
		status = XA_OK;
	    }

	    if (status == XA_OK)
		cursor = 0;
	} catch (...) {
	    // TODO: log it
	}

	EnterCriticalSection(&rmLock);
	active = false;
    }
    else {
	// TMSTARTRSCAN not set, is there an existing scan to work from?
	if (cursor == -1)
	    return XAER_INVAL;
    }

    if (status != XA_OK)
	return status;
    
    INT actualCount = count;
    if (count > 0) {
	int nAvailable = (int) inDoubtXids.size() - cursor;
	if (nAvailable < count)
	    actualCount = nAvailable;

	for (int i = 0; i < actualCount; i++) {
	    Xid& qpidXid = inDoubtXids[i + cursor];
	    QpidToXa(qpidXid, xids[i]);
	}
    }

    if (flags & TMENDRSCAN) {
	cursor = -1;
	inDoubtXids.clear();
    }

    return actualCount;
}


// Call with lock held

ResourceManager* findRm(int rmid) {
    if (rmMap.find(rmid) == rmMap.end()) {
	return NULL;
    }
    return rmMap[rmid];
}


INT __cdecl xa_open (char *xa_info, int rmid, long flags) {
    if (flags & TMASYNC)
	return XAER_ASYNC;

    INT rv = XAER_RMERR;
    EnterCriticalSection(&rmLock);

    ResourceManager* rmp = findRm(rmid);
    if (rmp != NULL) {
	// error: already in use
	rv = XAER_PROTO;
    }
    else {
	std::string brokerHost;
	int brokerPort;
	std::string username;
	std::string password;
	bool ssl;
	bool saslPlain;

	if (parseDsn(xa_info, brokerHost, brokerPort, ssl, saslPlain, username, password)) {

	    try {
		rmp = new ResourceManager(rmid, brokerHost, brokerPort, ssl, saslPlain, username, password);

		rv = rmp->open();
		if (rv != XA_OK) {
		    delete (rmp);
		}
		else {
		    rmMap[rmid] = rmp;
		}
	    } catch (...) {}
	}
	else {
	    rv = XAER_INVAL;
	}
    }

    LeaveCriticalSection(&rmLock);
    return rv;
}


INT __cdecl xa_close (char *xa_info, int rmid, long flags) {
    if (flags & TMASYNC)
	return XAER_ASYNC;

    INT rv = XAER_RMERR;

    EnterCriticalSection(&rmLock);
    ResourceManager* rmp = findRm(rmid);

    if (rmp == NULL) {
	// can close multiple times
	rv = XA_OK;
    }
    else {
	rv = rmp->close();
	rmMap.erase(rmid);
	try {
	    delete (rmp);
	} catch (...) {
	    // TODO: log it
	}
    }
	
    LeaveCriticalSection(&rmLock);
    return rv;
}


INT __cdecl xa_commit (XID *xid, int rmid, long flags) {
    if (flags & TMASYNC)
	return XAER_ASYNC;

    INT rv = XAER_RMFAIL;

    EnterCriticalSection(&rmLock);
    ResourceManager* rmp = findRm(rmid);

    if (rmp == NULL) {
	rv = XAER_INVAL;
    }
    else {
	rv = rmp->commit(xid);
    }

    LeaveCriticalSection(&rmLock);
    return rv;
}


INT __cdecl xa_rollback (XID *xid, int rmid, long flags) {
    if (flags & TMASYNC)
	return XAER_ASYNC;

    INT rv = XAER_RMFAIL;

    EnterCriticalSection(&rmLock);
    ResourceManager* rmp = findRm(rmid);

    if (rmp == NULL) {
	rv = XAER_INVAL;
    }
    else {
	rv = rmp->rollback(xid);
    }
	
    LeaveCriticalSection(&rmLock);
    return rv;
}


INT __cdecl xa_recover (XID *xids, long count, int rmid, long flags) {
    INT rv = XAER_RMFAIL;

    EnterCriticalSection(&rmLock);
    ResourceManager* rmp = findRm(rmid);

    if (rmp == NULL) {
	rv = XAER_PROTO;
    }
    else {
	rv = rmp->recover(xids, count, flags);
    }
	
    LeaveCriticalSection(&rmLock);
    return rv;
}


INT __cdecl xa_start (XID *xid, int rmid, long flags) {
    // not used in recovery
    return XAER_PROTO;
}


INT __cdecl xa_end (XID *xid, int rmid, long flags) {
    // not used in recovery
    return XAER_PROTO;
}


INT __cdecl xa_prepare (XID *xid, int rmid, long flags) {
    // not used in recovery
    return XAER_PROTO;
}


INT __cdecl xa_forget (XID *xid, int rmid, long flags) {
    // not used in recovery
    return XAER_PROTO;
}


INT __cdecl xa_complete (int *handle, int *retval, int rmid, long flags) {
    // not used in recovery
    return XAER_PROTO;
}



xa_switch_t xaSwitch;

HRESULT __cdecl GetQpidXaSwitch (DWORD XaSwitchFlags, xa_switch_t ** ppXaSwitch)
{
    // needed for now due to implicit use of FreeLibrary in WSACleanup() in qpid/cpp/src/qpid/sys/windows/Socket.cpp
    pinDll();

    if (xaSwitch.xa_open_entry != xa_open) {

	xaSwitch.xa_open_entry = xa_open;
	xaSwitch.xa_close_entry = xa_close;
	xaSwitch.xa_start_entry = xa_start;
	xaSwitch.xa_end_entry = xa_end;
	xaSwitch.xa_prepare_entry = xa_prepare;
	xaSwitch.xa_commit_entry = xa_commit;
	xaSwitch.xa_rollback_entry = xa_rollback;
	xaSwitch.xa_recover_entry = xa_recover;
	xaSwitch.xa_forget_entry = xa_forget;
	xaSwitch.xa_complete_entry = xa_complete;

	strcpy_s(xaSwitch.name, RMNAMESZ, "qpidxarm");
	xaSwitch.flags = TMNOMIGRATE;
	xaSwitch.version = 0;
    }
    *ppXaSwitch = &xaSwitch;
    return S_OK;
}




}}} // namespace Apache::Qpid::DtcPlugin


// GetXaSwitch

extern "C" {

    __declspec(dllexport) HRESULT __cdecl GetXaSwitch (DWORD XaSwitchFlags, xa_switch_t ** ppXaSwitch)
    {
	return Apache::Qpid::DtcPlugin::GetQpidXaSwitch (XaSwitchFlags, ppXaSwitch);
    }
}


// dllmain

BOOL APIENTRY DllMain( HMODULE hModule,
                       DWORD  ul_reason_for_call,
                       LPVOID lpReserved)
{

	switch (ul_reason_for_call)
	{
	case DLL_PROCESS_ATTACH:
	    InitializeCriticalSection(&Apache::Qpid::DtcPlugin::rmLock);
	    Apache::Qpid::DtcPlugin::thisDll = hModule;
	    break;

	case DLL_PROCESS_DETACH:
	    DeleteCriticalSection(&Apache::Qpid::DtcPlugin::rmLock);	
	    break;

	case DLL_THREAD_ATTACH:
	case DLL_THREAD_DETACH:
		break;
	}
	return TRUE;
}

