#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
from qpid.testlib import TestBase010
from qpid.datatypes import Message
from qpid.queue import Empty
from qpid.util import URL
from time import sleep


class _FedBroker(object):
    """
    A proxy object for a remote broker.  Contains connection and management
    state.
    """
    def __init__(self, host, port, 
                 conn=None, session=None, qmf_broker=None):
        self.host = host
        self.port = port
        self.url = "%s:%d" % (host, port)
        self.client_conn = None
        self.client_session = None
        self.qmf_broker = None
        self.qmf_object = None
        if conn is not None:
            self.client_conn = conn
        if session is not None:
            self.client_session = session
        if qmf_broker is not None:
            self.qmf_broker = qmf_broker


class FederationTests(TestBase010):

    def remote_host(self):
        return self.defines.get("remote-host", "localhost")

    def remote_port(self):
        return int(self.defines["remote-port"])

    def verify_cleanup(self):
        attempts = 0
        total = len(self.qmf.getObjects(_class="bridge")) + len(self.qmf.getObjects(_class="link"))
        while total > 0:
            attempts += 1
            if attempts >= 10:
                self.fail("Bridges and links didn't clean up")
                return
            sleep(1)
            total = len(self.qmf.getObjects(_class="bridge")) + len(self.qmf.getObjects(_class="link"))

    def _setup_brokers(self):
        ports = [self.remote_port()]
        extra = self.defines.get("extra-brokers")
        if extra:
            for p in extra.split():
                ports.append(int(p))

        # broker[0] has already been set up.
        self._brokers = [_FedBroker(self.broker.host,
                                    self.broker.port,
                                    self.conn,
                                    self.session,
                                    self.qmf_broker)]
        self._brokers[0].qmf_object = self.qmf.getObjects(_class="broker")[0]

        # setup remaining brokers
        for _p in ports:
            _b = _FedBroker(self.remote_host(), _p)
            _b.client_conn = self.connect(host=self.remote_host(), port=_p)
            _b.client_session = _b.client_conn.session("Fed_client_session_" + str(_p))
            _b.qmf_broker = self.qmf.addBroker(_b.url)
            for _bo in self.qmf.getObjects(_class="broker"):
                if _bo.getBroker().getUrl() == _b.qmf_broker.getUrl():
                    _b.qmf_object = _bo
                    break
            self._brokers.append(_b)

    def _teardown_brokers(self):
        """ Un-does _setup_brokers()
        """
        # broker[0] is configured at test setup, so it must remain configured
        for _b in self._brokers[1:]:
            self.qmf.delBroker(_b.qmf_broker)
            if not _b.client_session.error():
                _b.client_session.close(timeout=10)
            _b.client_conn.close(timeout=10)


    def test_bridge_create_and_close(self):
        self.startQmf();
        qmf = self.qmf

        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "amq.direct", "amq.direct", "my-key", "", "", False, False, False, 0)
        self.assertEqual(result.status, 0)

        bridge = qmf.getObjects(_class="bridge")[0]
        result = bridge.close()
        self.assertEqual(result.status, 0)

        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_pull_from_exchange(self):
        session = self.session
        
        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "amq.direct", "amq.fanout", "my-key", "", "", False, False, False, 0)
        self.assertEqual(result.status, 0)

        bridge = qmf.getObjects(_class="bridge")[0]

        #setup queue to receive messages from local broker
        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="amq.fanout")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")
        sleep(6)

        #send messages to remote broker and confirm it is routed to local broker
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_pull_from_exchange")

        for i in range(1, 11):
            dp = r_session.delivery_properties(routing_key="my-key")
            r_session.message_transfer(destination="amq.direct", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_push_to_exchange(self):
        session = self.session
        
        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "amq.direct", "amq.fanout", "my-key", "", "", False, True, False, 0)
        self.assertEqual(result.status, 0)

        bridge = qmf.getObjects(_class="bridge")[0]

        #setup queue to receive messages from remote broker
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_push_to_exchange")
        r_session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        r_session.exchange_bind(queue="fed1", exchange="amq.fanout")
        self.subscribe(session=r_session, queue="fed1", destination="f1")
        queue = r_session.incoming("f1")
        sleep(6)

        #send messages to local broker and confirm it is routed to remote broker
        for i in range(1, 11):
            dp = session.delivery_properties(routing_key="my-key")
            session.message_transfer(destination="amq.direct", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_pull_from_queue(self):
        session = self.session

        #setup queue on remote broker and add some messages
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_pull_from_queue")
        r_session.queue_declare(queue="my-bridge-queue", auto_delete=True)
        for i in range(1, 6):
            dp = r_session.delivery_properties(routing_key="my-bridge-queue")
            r_session.message_transfer(message=Message(dp, "Message %d" % i))

        #setup queue to receive messages from local broker
        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="amq.fanout")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "my-bridge-queue", "amq.fanout", "my-key", "", "", True, False, False, 1)
        self.assertEqual(result.status, 0)

        bridge = qmf.getObjects(_class="bridge")[0]
        sleep(3)

        #add some more messages (i.e. after bridge was created)
        for i in range(6, 11):
            dp = r_session.delivery_properties(routing_key="my-bridge-queue")
            r_session.message_transfer(message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            try:
                msg = queue.get(timeout=5)
                self.assertEqual("Message %d" % i, msg.body)
            except Empty:
                self.fail("Failed to find expected message containing 'Message %d'" % i)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_tracing_automatic(self):
        remoteUrl = "%s:%d" % (self.remote_host(), self.remote_port())
        self.startQmf()
        l_broker = self.qmf_broker
        r_broker = self.qmf.addBroker(remoteUrl)

        l_brokerObj = self.qmf.getObjects(_class="broker", _broker=l_broker)[0]
        r_brokerObj = self.qmf.getObjects(_class="broker", _broker=r_broker)[0]

        l_res = l_brokerObj.connect(self.remote_host(), self.remote_port(),     False, "PLAIN", "guest", "guest", "tcp")
        r_res = r_brokerObj.connect(self.broker.host, self.broker.port, False, "PLAIN", "guest", "guest", "tcp")

        self.assertEqual(l_res.status, 0)
        self.assertEqual(r_res.status, 0)

        l_link = self.qmf.getObjects(_class="link", _broker=l_broker)[0]
        r_link = self.qmf.getObjects(_class="link", _broker=r_broker)[0]

        l_res = l_link.bridge(False, "amq.direct", "amq.direct", "key", "", "", False, False, False, 0)
        r_res = r_link.bridge(False, "amq.direct", "amq.direct", "key", "", "", False, False, False, 0)

        self.assertEqual(l_res.status, 0)
        self.assertEqual(r_res.status, 0)

        count = 0
        while l_link.state != "Operational" or r_link.state != "Operational":
            count += 1
            if count > 10:
                self.fail("Fed links didn't become operational after 10 seconds")
            sleep(1)
            l_link = self.qmf.getObjects(_class="link", _broker=l_broker)[0]
            r_link = self.qmf.getObjects(_class="link", _broker=r_broker)[0]
        sleep(3)

        #setup queue to receive messages from local broker
        session = self.session
        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="amq.direct", binding_key="key")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        #setup queue on remote broker and add some messages
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_trace")
        for i in range(1, 11):
            dp = r_session.delivery_properties(routing_key="key")
            r_session.message_transfer(destination="amq.direct", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            try:
                msg = queue.get(timeout=5)
                self.assertEqual("Message %d" % i, msg.body)
            except Empty:
                self.fail("Failed to find expected message containing 'Message %d'" % i)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

    def test_tracing(self):
        session = self.session
        
        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "amq.direct", "amq.fanout", "my-key", "my-bridge-id",
                             "exclude-me,also-exclude-me", False, False, False, 0)
        self.assertEqual(result.status, 0)
        bridge = qmf.getObjects(_class="bridge")[0]

        #setup queue to receive messages from local broker
        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="amq.fanout")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")
        sleep(6)

        #send messages to remote broker and confirm it is routed to local broker
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_tracing")

        trace = [None, "exclude-me", "a,exclude-me,b", "also-exclude-me,c", "dont-exclude-me"]
        body = ["yes", "first-bad", "second-bad", "third-bad", "yes"]
        for b, t in zip(body, trace):
            headers = {}
            if (t): headers["x-qpid.trace"]=t
            dp = r_session.delivery_properties(routing_key="my-key")
            mp = r_session.message_properties(application_headers=headers)
            r_session.message_transfer(destination="amq.direct", message=Message(dp, mp, b))

        for e in ["my-bridge-id", "dont-exclude-me,my-bridge-id"]:
            msg = queue.get(timeout=5)
            self.assertEqual("yes", msg.body)
            self.assertEqual(e, self.getAppHeader(msg, "x-qpid.trace"))

        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_dynamic_fanout(self):
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_fanout")

        session.exchange_declare(exchange="fed.fanout", type="fanout")
        r_session.exchange_declare(exchange="fed.fanout", type="fanout")

        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.fanout", "fed.fanout", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        bridge = qmf.getObjects(_class="bridge")[0]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="fed.fanout")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        for i in range(1, 11):
            dp = r_session.delivery_properties()
            r_session.message_transfer(destination="fed.fanout", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()


    def test_dynamic_direct(self):
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_direct")

        session.exchange_declare(exchange="fed.direct", type="direct")
        r_session.exchange_declare(exchange="fed.direct", type="direct")

        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.direct", "fed.direct", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        bridge = qmf.getObjects(_class="bridge")[0]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="fed.direct", binding_key="fd-key")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        for i in range(1, 11):
            dp = r_session.delivery_properties(routing_key="fd-key")
            r_session.message_transfer(destination="fed.direct", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_dynamic_topic(self):
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_topic")

        session.exchange_declare(exchange="fed.topic", type="topic")
        r_session.exchange_declare(exchange="fed.topic", type="topic")

        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.topic", "fed.topic", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        bridge = qmf.getObjects(_class="bridge")[0]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="fed.topic", binding_key="ft-key.#")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        for i in range(1, 11):
            dp = r_session.delivery_properties(routing_key="ft-key.one.two")
            r_session.message_transfer(destination="fed.topic", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_dynamic_topic_reorigin(self):
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_topic_reorigin")

        session.exchange_declare(exchange="fed.topic_reorigin", type="topic")
        r_session.exchange_declare(exchange="fed.topic_reorigin", type="topic")

        session.exchange_declare(exchange="fed.topic_reorigin_2", type="topic")
        r_session.exchange_declare(exchange="fed.topic_reorigin_2", type="topic")

        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        session.queue_declare(queue="fed2", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed2", exchange="fed.topic_reorigin_2", binding_key="ft-key.one.#")
        self.subscribe(queue="fed2", destination="f2")
        queue2 = session.incoming("f2")

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.topic_reorigin", "fed.topic_reorigin", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        result = link.bridge(False, "fed.topic_reorigin_2", "fed.topic_reorigin_2", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)

        bridge = qmf.getObjects(_class="bridge")[0]
        bridge2 = qmf.getObjects(_class="bridge")[1]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="fed.topic_reorigin", binding_key="ft-key.#")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        for i in range(1, 11):
            dp = r_session.delivery_properties(routing_key="ft-key.one.two")
            r_session.message_transfer(destination="fed.topic_reorigin", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = bridge2.close()
        self.assertEqual(result.status, 0)

        # extra check: verify we don't leak bridge objects - keep the link
        # around and verify the bridge count has gone to zero

        attempts = 0
        bridgeCount = len(qmf.getObjects(_class="bridge"))
        while bridgeCount > 0:
            attempts += 1
            if attempts >= 5:
                self.fail("Bridges didn't clean up")
                return
            sleep(1)
            bridgeCount = len(qmf.getObjects(_class="bridge"))

        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()
        
    def test_dynamic_direct_reorigin(self):
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_direct_reorigin")

        session.exchange_declare(exchange="fed.direct_reorigin", type="direct")
        r_session.exchange_declare(exchange="fed.direct_reorigin", type="direct")

        session.exchange_declare(exchange="fed.direct_reorigin_2", type="direct")
        r_session.exchange_declare(exchange="fed.direct_reorigin_2", type="direct")

        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        session.queue_declare(queue="fed2", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed2", exchange="fed.direct_reorigin_2", binding_key="ft-key.two")
        self.subscribe(queue="fed2", destination="f2")
        queue2 = session.incoming("f2")

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.direct_reorigin", "fed.direct_reorigin", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        result = link.bridge(False, "fed.direct_reorigin_2", "fed.direct_reorigin_2", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)

        bridge = qmf.getObjects(_class="bridge")[0]
        bridge2 = qmf.getObjects(_class="bridge")[1]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="fed.direct_reorigin", binding_key="ft-key.one")
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        for i in range(1, 11):
            dp = r_session.delivery_properties(routing_key="ft-key.one")
            r_session.message_transfer(destination="fed.direct_reorigin", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        
        # Extra test: don't explicitly close() bridge2.  When the link is closed,
        # it should clean up bridge2 automagically.  verify_cleanup() will detect
        # if bridge2 isn't cleaned up and will fail the test.
        #
        #result = bridge2.close()
        #self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_dynamic_headers(self):
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_headers")

        session.exchange_declare(exchange="fed.headers", type="headers")
        r_session.exchange_declare(exchange="fed.headers", type="headers")

        self.startQmf()
        qmf = self.qmf

        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.headers", "fed.headers", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        bridge = qmf.getObjects(_class="bridge")[0]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="fed.headers", binding_key="key1", arguments={'x-match':'any', 'class':'first'})
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        props = r_session.message_properties(application_headers={'class':'first'})
        for i in range(1, 11):
            r_session.message_transfer(destination="fed.headers", message=Message(props, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            content = msg.body
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_dynamic_headers_reorigin(self):
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_headers_reorigin")

        session.exchange_declare(exchange="fed.headers_reorigin", type="headers")
        r_session.exchange_declare(exchange="fed.headers_reorigin", type="headers")

        session.exchange_declare(exchange="fed.headers_reorigin_2", type="headers")
        r_session.exchange_declare(exchange="fed.headers_reorigin_2", type="headers")

        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        session.queue_declare(queue="fed2", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed2", exchange="fed.headers_reorigin_2", binding_key="key2", arguments={'x-match':'any', 'class':'second'})
        self.subscribe(queue="fed2", destination="f2")
        queue2 = session.incoming("f2")

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.headers_reorigin", "fed.headers_reorigin", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        result = link.bridge(False, "fed.headers_reorigin_2", "fed.headers_reorigin_2", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)

        bridge = qmf.getObjects(_class="bridge")[0]
        bridge2 = qmf.getObjects(_class="bridge")[1]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="fed.headers_reorigin", binding_key="key1", arguments={'x-match':'any', 'class':'first'})
        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        props = r_session.message_properties(application_headers={'class':'first'})
        for i in range(1, 11):
            r_session.message_transfer(destination="fed.headers_reorigin", message=Message(props, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)

        # Extra test: don't explicitly close() bridge2.  When the link is closed,
        # it should clean up bridge2 automagically.  verify_cleanup() will detect
        # if bridge2 isn't cleaned up and will fail the test.
        #
        #result = bridge2.close()
        #self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_dynamic_headers_unbind(self):
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_headers_unbind")

        session.exchange_declare(exchange="fed.headers_unbind", type="headers")
        r_session.exchange_declare(exchange="fed.headers_unbind", type="headers")

        self.startQmf()
        qmf = self.qmf

        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.headers_unbind", "fed.headers_unbind", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        bridge = qmf.getObjects(_class="bridge")[0]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        queue = qmf.getObjects(_class="queue", name="fed1")[0]
        queue.update()
        self.assertEqual(queue.bindingCount, 1,
                         "bindings not accounted for (expected 1, got %d)" % queue.bindingCount)

        session.exchange_bind(queue="fed1", exchange="fed.headers_unbind", binding_key="key1", arguments={'x-match':'any', 'class':'first'})
        queue.update()
        self.assertEqual(queue.bindingCount, 2,
                         "bindings not accounted for (expected 2, got %d)" % queue.bindingCount)

        session.exchange_unbind(queue="fed1", exchange="fed.headers_unbind", binding_key="key1")
        queue.update()
        self.assertEqual(queue.bindingCount, 1,
                         "bindings not accounted for (expected 1, got %d)" % queue.bindingCount)

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()

    def test_dynamic_topic_nodup(self):
        """Verify that a message whose routing key matches more than one
        binding does not get duplicated to the same queue.
        """
        session = self.session
        r_conn = self.connect(host=self.remote_host(), port=self.remote_port())
        r_session = r_conn.session("test_dynamic_topic_nodup")

        session.exchange_declare(exchange="fed.topic", type="topic")
        r_session.exchange_declare(exchange="fed.topic", type="topic")

        self.startQmf()
        qmf = self.qmf
        broker = qmf.getObjects(_class="broker")[0]
        result = broker.connect(self.remote_host(), self.remote_port(), False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        link = qmf.getObjects(_class="link")[0]
        result = link.bridge(False, "fed.topic", "fed.topic", "", "", "", False, False, True, 0)
        self.assertEqual(result.status, 0)
        bridge = qmf.getObjects(_class="bridge")[0]
        sleep(5)

        session.queue_declare(queue="fed1", exclusive=True, auto_delete=True)
        session.exchange_bind(queue="fed1", exchange="fed.topic", binding_key="red.*")
        session.exchange_bind(queue="fed1", exchange="fed.topic", binding_key="*.herring")

        self.subscribe(queue="fed1", destination="f1")
        queue = session.incoming("f1")

        for i in range(1, 11):
            dp = r_session.delivery_properties(routing_key="red.herring")
            r_session.message_transfer(destination="fed.topic", message=Message(dp, "Message %d" % i))

        for i in range(1, 11):
            msg = queue.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
        try:
            extra = queue.get(timeout=1)
            self.fail("Got unexpected message in queue: " + extra.body)
        except Empty: None

        result = bridge.close()
        self.assertEqual(result.status, 0)
        result = link.close()
        self.assertEqual(result.status, 0)

        self.verify_cleanup()


    def test_dynamic_direct_route_prop(self):
        """ Set up a tree of uni-directional routes across the direct exchange.
        Bind the same key to the same queues on the leaf nodes.  Verify a
        message sent with the routing key transverses the tree an arrives at
        each leaf.  Remove one leaf's queue, and verify that messages still
        reach the other leaf.

        Route Topology:

                    +---> B2  queue:"test-queue", binding key:"spudboy"
        B0 --> B1 --+
                    +---> B3  queue:"test-queue", binding key:"spudboy"
        """
        session = self.session

        # create the federation

        self.startQmf()
        qmf = self.qmf

        self._setup_brokers()

        # create direct exchange on each broker, and retrieve the corresponding
        # management object for that exchange

        exchanges=[]
        for _b in self._brokers:
            _b.client_session.exchange_declare(exchange="fedX.direct", type="direct")
            self.assertEqual(_b.client_session.exchange_query(name="fedX.direct").type,
                             "direct", "exchange_declare failed!")
            # pull the exchange out of qmf...
            retries = 0
            my_exchange = None
            while my_exchange is None:
                objs = qmf.getObjects(_broker=_b.qmf_broker, _class="exchange")
                for ooo in objs:
                    if ooo.name == "fedX.direct":
                        my_exchange = ooo
                        break
                if my_exchange is None:
                    retries += 1
                    self.failIfEqual(retries, 10,
                                     "QMF failed to find new exchange!")
                    sleep(1)
            exchanges.append(my_exchange)

        self.assertEqual(len(exchanges), len(self._brokers), "Exchange creation failed!")

        # connect B0 --> B1
        result = self._brokers[1].qmf_object.connect(self._brokers[0].host,
                                                     self._brokers[0].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # connect B1 --> B2
        result = self._brokers[2].qmf_object.connect(self._brokers[1].host,
                                                     self._brokers[1].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # connect B1 --> B3
        result = self._brokers[3].qmf_object.connect(self._brokers[1].host, 
                                                     self._brokers[1].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # for each link, bridge the "fedX.direct" exchanges:

        for _l in qmf.getObjects(_class="link"):
            # print("Link=%s:%s %s" % (_l.host, _l.port, str(_l.getBroker())))
            result = _l.bridge(False,  # durable
                                 "fedX.direct",  # src
                                 "fedX.direct",  # dst
                                 "",  # key
                                 "",  # tag
                                 "",  # excludes
                                 False, # srcIsQueue
                                 False, # srcIsLocal
                                 True,  # dynamic
                                 0)     # sync
            self.assertEqual(result.status, 0)

        # create a queue on B2, bound to "spudboy"
        self._brokers[2].client_session.queue_declare(queue="fedX1", exclusive=True, auto_delete=True)
        self._brokers[2].client_session.exchange_bind(queue="fedX1", exchange="fedX.direct", binding_key="spudboy")

        # create a queue on B3, bound to "spudboy"
        self._brokers[3].client_session.queue_declare(queue="fedX1", exclusive=True, auto_delete=True)
        self._brokers[3].client_session.exchange_bind(queue="fedX1", exchange="fedX.direct", binding_key="spudboy")

        # subscribe to messages arriving on B2's queue
        self.subscribe(self._brokers[2].client_session, queue="fedX1", destination="f1")
        queue_2 = self._brokers[2].client_session.incoming("f1")

        # subscribe to messages arriving on B3's queue
        self.subscribe(self._brokers[3].client_session, queue="fedX1", destination="f1")
        queue_3 = self._brokers[3].client_session.incoming("f1")

        # wait until the binding key has propagated to each broker (twice at
        # broker B1).  Work backwards from binding brokers.

        binding_counts = [1, 2, 1, 1]
        self.assertEqual(len(binding_counts), len(exchanges), "Update Test!")
        for i in range(3,-1,-1):
            retries = 0
            exchanges[i].update()
            while exchanges[i].bindingCount < binding_counts[i]:
                retries += 1
                self.failIfEqual(retries, 10,
                                 "binding failed to propagate to broker %d"
                                 % i)
                sleep(3)
                exchanges[i].update()

        # send 10 msgs from B0
        for i in range(1, 11):
            dp = self._brokers[0].client_session.delivery_properties(routing_key="spudboy")
            self._brokers[0].client_session.message_transfer(destination="fedX.direct", message=Message(dp, "Message %d" % i))

        # wait for 10 messages to be forwarded from B0->B1,
        # 10 messages from B1->B2,
        # and 10 messages from B1->B3
        retries = 0
        for ex in exchanges:
            ex.update()
        while (exchanges[0].msgReceives != 10 or exchanges[0].msgRoutes != 10 or
               exchanges[1].msgReceives != 10 or exchanges[1].msgRoutes != 20 or
               exchanges[2].msgReceives != 10 or exchanges[2].msgRoutes != 10 or
               exchanges[3].msgReceives != 10 or exchanges[3].msgRoutes != 10):
            retries += 1
            self.failIfEqual(retries, 10,
                             "federation failed to route msgs %d:%d %d:%d %d:%d %d:%d"
                             % (exchanges[0].msgReceives,
                                exchanges[0].msgRoutes,
                                exchanges[1].msgReceives,
                                exchanges[1].msgRoutes,
                                exchanges[2].msgReceives,
                                exchanges[2].msgRoutes,
                                exchanges[3].msgReceives,
                                exchanges[3].msgRoutes))
            sleep(1)
            for ex in exchanges:
                ex.update()

        # get exactly 10 msgs on B2 and B3
        for i in range(1, 11):
            msg = queue_2.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
            msg = queue_3.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)

        try:
            extra = queue_2.get(timeout=1)
            self.fail("Got unexpected message in queue_2: " + extra.body)
        except Empty: None

        try:
            extra = queue_3.get(timeout=1)
            self.fail("Got unexpected message in queue_3: " + extra.body)
        except Empty: None


        # tear down the queue on B2
        self._brokers[2].client_session.exchange_unbind(queue="fedX1", exchange="fedX.direct", binding_key="spudboy")
        self._brokers[2].client_session.message_cancel(destination="f1")
        self._brokers[2].client_session.queue_delete(queue="fedX1")

        # @todo - restore code when QPID-2499 fixed!!
        sleep(6)
        # wait for the binding count on B1 to drop from 2 to 1
        # retries = 0
        # exchanges[1].update()
        # while exchanges[1].bindingCount != 1:
        #     retries += 1
        #     self.failIfEqual(retries, 10,
        #                      "unbinding failed to propagate to broker B1: %d"
        #                      % exchanges[1].bindingCount)
        #     sleep(1)
        #     exchanges[1].update()

        # send 10 msgs from B0
        for i in range(1, 11):
            dp = self._brokers[0].client_session.delivery_properties(routing_key="spudboy")
            self._brokers[0].client_session.message_transfer(destination="fedX.direct", message=Message(dp, "Message %d" % i))

        # verify messages are forwarded to B3 only
        # note: why exchanges[1].msgRoutes == 40???, not 20???  QPID-2499?
        retries = 0
        for ex in exchanges:
            ex.update()
        while (exchanges[0].msgReceives != 20 or exchanges[0].msgRoutes != 20 or
               exchanges[1].msgReceives != 20 or exchanges[1].msgRoutes != 40 or
               exchanges[2].msgReceives != 20 or exchanges[2].msgDrops != 10 or exchanges[2].msgRoutes != 10 or
               exchanges[3].msgReceives != 20 or exchanges[3].msgRoutes != 20):
            retries += 1
            self.failIfEqual(retries, 10,
                             "federation failed to route more msgs %d:%d %d:%d %d:%d %d:%d"
                             % (exchanges[0].msgReceives,
                                exchanges[0].msgRoutes,
                                exchanges[1].msgReceives,
                                exchanges[1].msgRoutes,
                                exchanges[2].msgReceives,
                                exchanges[2].msgRoutes,
                                exchanges[3].msgReceives,
                                exchanges[3].msgRoutes))
            sleep(1)
            for ex in exchanges:
                ex.update()

        # get exactly 10 msgs on B3 only
        for i in range(1, 11):
            msg = queue_3.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)

        try:
            extra = queue_3.get(timeout=1)
            self.fail("Got unexpected message in queue_3: " + extra.body)
        except Empty: None

        # cleanup

        self._brokers[3].client_session.exchange_unbind(queue="fedX1", exchange="fedX.direct", binding_key="spudboy")
        self._brokers[3].client_session.message_cancel(destination="f1")
        self._brokers[3].client_session.queue_delete(queue="fedX1")

        for _b in qmf.getObjects(_class="bridge"):
            result = _b.close()
            self.assertEqual(result.status, 0)

        for _l in qmf.getObjects(_class="link"):
            result = _l.close()
            self.assertEqual(result.status, 0)

        for _b in self._brokers:
            _b.client_session.exchange_delete(exchange="fedX.direct")

        self._teardown_brokers()

        self.verify_cleanup()

    def test_dynamic_topic_route_prop(self):
        """ Set up a tree of uni-directional routes across a topic exchange.
        Bind the same key to the same queues on the leaf nodes.  Verify a
        message sent with the routing key transverses the tree an arrives at
        each leaf.  Remove one leaf's queue, and verify that messages still
        reach the other leaf.

        Route Topology:

                    +---> B2  queue:"test-queue", binding key:"spud.*"
        B0 --> B1 --+
                    +---> B3  queue:"test-queue", binding key:"spud.*"
        """
        session = self.session

        # create the federation

        self.startQmf()
        qmf = self.qmf

        self._setup_brokers()

        # create exchange on each broker, and retrieve the corresponding
        # management object for that exchange

        exchanges=[]
        for _b in self._brokers:
            _b.client_session.exchange_declare(exchange="fedX.topic", type="topic")
            self.assertEqual(_b.client_session.exchange_query(name="fedX.topic").type,
                             "topic", "exchange_declare failed!")
            # pull the exchange out of qmf...
            retries = 0
            my_exchange = None
            while my_exchange is None:
                objs = qmf.getObjects(_broker=_b.qmf_broker, _class="exchange")
                for ooo in objs:
                    if ooo.name == "fedX.topic":
                        my_exchange = ooo
                        break
                if my_exchange is None:
                    retries += 1
                    self.failIfEqual(retries, 10,
                                     "QMF failed to find new exchange!")
                    sleep(1)
            exchanges.append(my_exchange)

        self.assertEqual(len(exchanges), len(self._brokers), "Exchange creation failed!")

        # connect B0 --> B1
        result = self._brokers[1].qmf_object.connect(self._brokers[0].host,
                                                     self._brokers[0].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # connect B1 --> B2
        result = self._brokers[2].qmf_object.connect(self._brokers[1].host,
                                                     self._brokers[1].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # connect B1 --> B3
        result = self._brokers[3].qmf_object.connect(self._brokers[1].host, 
                                                     self._brokers[1].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # for each link, bridge the "fedX.topic" exchanges:

        for _l in qmf.getObjects(_class="link"):
            # print("Link=%s:%s %s" % (_l.host, _l.port, str(_l.getBroker())))
            result = _l.bridge(False,  # durable
                                 "fedX.topic",  # src
                                 "fedX.topic",  # dst
                                 "",  # key
                                 "",  # tag
                                 "",  # excludes
                                 False, # srcIsQueue
                                 False, # srcIsLocal
                                 True,  # dynamic
                                 0)     # sync
            self.assertEqual(result.status, 0)

        # create a queue on B2, bound to "spudboy"
        self._brokers[2].client_session.queue_declare(queue="fedX1", exclusive=True, auto_delete=True)
        self._brokers[2].client_session.exchange_bind(queue="fedX1", exchange="fedX.topic", binding_key="spud.*")

        # create a queue on B3, bound to "spudboy"
        self._brokers[3].client_session.queue_declare(queue="fedX1", exclusive=True, auto_delete=True)
        self._brokers[3].client_session.exchange_bind(queue="fedX1", exchange="fedX.topic", binding_key="spud.*")

        # subscribe to messages arriving on B2's queue
        self.subscribe(self._brokers[2].client_session, queue="fedX1", destination="f1")
        queue_2 = self._brokers[2].client_session.incoming("f1")

        # subscribe to messages arriving on B3's queue
        self.subscribe(self._brokers[3].client_session, queue="fedX1", destination="f1")
        queue_3 = self._brokers[3].client_session.incoming("f1")

        # wait until the binding key has propagated to each broker (twice at
        # broker B1).  Work backwards from binding brokers.

        binding_counts = [1, 2, 1, 1]
        self.assertEqual(len(binding_counts), len(exchanges), "Update Test!")
        for i in range(3,-1,-1):
            retries = 0
            exchanges[i].update()
            while exchanges[i].bindingCount < binding_counts[i]:
                retries += 1
                self.failIfEqual(retries, 10,
                                 "binding failed to propagate to broker %d"
                                 % i)
                sleep(3)
                exchanges[i].update()

        # send 10 msgs from B0
        for i in range(1, 11):
            dp = self._brokers[0].client_session.delivery_properties(routing_key="spud.boy")
            self._brokers[0].client_session.message_transfer(destination="fedX.topic", message=Message(dp, "Message %d" % i))

        # wait for 10 messages to be forwarded from B0->B1,
        # 10 messages from B1->B2,
        # and 10 messages from B1->B3
        retries = 0
        for ex in exchanges:
            ex.update()
        while (exchanges[0].msgReceives != 10 or exchanges[0].msgRoutes != 10 or
               exchanges[1].msgReceives != 10 or exchanges[1].msgRoutes != 20 or
               exchanges[2].msgReceives != 10 or exchanges[2].msgRoutes != 10 or
               exchanges[3].msgReceives != 10 or exchanges[3].msgRoutes != 10):
            retries += 1
            self.failIfEqual(retries, 10,
                             "federation failed to route msgs %d:%d %d:%d %d:%d %d:%d"
                             % (exchanges[0].msgReceives,
                                exchanges[0].msgRoutes,
                                exchanges[1].msgReceives,
                                exchanges[1].msgRoutes,
                                exchanges[2].msgReceives,
                                exchanges[2].msgRoutes,
                                exchanges[3].msgReceives,
                                exchanges[3].msgRoutes))
            sleep(1)
            for ex in exchanges:
                ex.update()

        # get exactly 10 msgs on B2 and B3
        for i in range(1, 11):
            msg = queue_2.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
            msg = queue_3.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)

        try:
            extra = queue_2.get(timeout=1)
            self.fail("Got unexpected message in queue_2: " + extra.body)
        except Empty: None

        try:
            extra = queue_3.get(timeout=1)
            self.fail("Got unexpected message in queue_3: " + extra.body)
        except Empty: None

        # tear down the queue on B2
        self._brokers[2].client_session.exchange_unbind(queue="fedX1", exchange="fedX.topic", binding_key="spud.*")
        self._brokers[2].client_session.message_cancel(destination="f1")
        self._brokers[2].client_session.queue_delete(queue="fedX1")

        # @todo - restore code when QPID-2499 fixed!!
        sleep(6)
        # wait for the binding count on B1 to drop from 2 to 1
        # retries = 0
        # exchanges[1].update()
        # while exchanges[1].bindingCount != 1:
        #     retries += 1
        #     self.failIfEqual(retries, 10,
        #                      "unbinding failed to propagate to broker B1: %d"
        #                      % exchanges[1].bindingCount)
        #     sleep(1)
        #     exchanges[1].update()

        # send 10 msgs from B0
        for i in range(1, 11):
            dp = self._brokers[0].client_session.delivery_properties(routing_key="spud.boy")
            self._brokers[0].client_session.message_transfer(destination="fedX.topic", message=Message(dp, "Message %d" % i))

        # verify messages are forwarded to B3 only
        # note: why exchanges[1].msgRoutes == 40???, not 20???  QPID-2499?
        retries = 0
        for ex in exchanges:
            ex.update()
        while (exchanges[0].msgReceives != 20 or exchanges[0].msgRoutes != 20 or
               exchanges[1].msgReceives != 20 or exchanges[1].msgRoutes != 40 or
               exchanges[2].msgReceives != 20 or exchanges[2].msgDrops != 10 or exchanges[2].msgRoutes != 10 or
               exchanges[3].msgReceives != 20 or exchanges[3].msgRoutes != 20):
            retries += 1
            self.failIfEqual(retries, 10,
                             "federation failed to route more msgs %d:%d %d:%d %d:%d %d:%d"
                             % (exchanges[0].msgReceives,
                                exchanges[0].msgRoutes,
                                exchanges[1].msgReceives,
                                exchanges[1].msgRoutes,
                                exchanges[2].msgReceives,
                                exchanges[2].msgRoutes,
                                exchanges[3].msgReceives,
                                exchanges[3].msgRoutes))
            sleep(1)
            for ex in exchanges:
                ex.update()

        # get exactly 10 msgs on B3 only
        for i in range(1, 11):
            msg = queue_3.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)

        try:
            extra = queue_3.get(timeout=1)
            self.fail("Got unexpected message in queue_3: " + extra.body)
        except Empty: None

        # cleanup

        self._brokers[3].client_session.exchange_unbind(queue="fedX1", exchange="fedX.topic", binding_key="spud.*")
        self._brokers[3].client_session.message_cancel(destination="f1")
        self._brokers[3].client_session.queue_delete(queue="fedX1")

        for _b in qmf.getObjects(_class="bridge"):
            result = _b.close()
            self.assertEqual(result.status, 0)

        for _l in qmf.getObjects(_class="link"):
            result = _l.close()
            self.assertEqual(result.status, 0)

        for _b in self._brokers:
            _b.client_session.exchange_delete(exchange="fedX.topic")

        self._teardown_brokers()

        self.verify_cleanup()


    def test_dynamic_fanout_route_prop(self):
        """ Set up a tree of uni-directional routes across a fanout exchange.
        Bind the same key to the same queues on the leaf nodes.  Verify a
        message sent with the routing key transverses the tree an arrives at
        each leaf.  Remove one leaf's queue, and verify that messages still
        reach the other leaf.

        Route Topology:

                    +---> B2  queue:"test-queue", binding key:"spud.*"
        B0 --> B1 --+
                    +---> B3  queue:"test-queue", binding key:"spud.*"
        """
        session = self.session

        # create the federation

        self.startQmf()
        qmf = self.qmf

        self._setup_brokers()

        # create fanout exchange on each broker, and retrieve the corresponding
        # management object for that exchange

        exchanges=[]
        for _b in self._brokers:
            _b.client_session.exchange_declare(exchange="fedX.fanout", type="fanout")
            self.assertEqual(_b.client_session.exchange_query(name="fedX.fanout").type,
                             "fanout", "exchange_declare failed!")
            # pull the exchange out of qmf...
            retries = 0
            my_exchange = None
            while my_exchange is None:
                objs = qmf.getObjects(_broker=_b.qmf_broker, _class="exchange")
                for ooo in objs:
                    if ooo.name == "fedX.fanout":
                        my_exchange = ooo
                        break
                if my_exchange is None:
                    retries += 1
                    self.failIfEqual(retries, 10,
                                     "QMF failed to find new exchange!")
                    sleep(1)
            exchanges.append(my_exchange)

        self.assertEqual(len(exchanges), len(self._brokers), "Exchange creation failed!")

        # connect B0 --> B1
        result = self._brokers[1].qmf_object.connect(self._brokers[0].host,
                                                     self._brokers[0].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # connect B1 --> B2
        result = self._brokers[2].qmf_object.connect(self._brokers[1].host,
                                                     self._brokers[1].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # connect B1 --> B3
        result = self._brokers[3].qmf_object.connect(self._brokers[1].host, 
                                                     self._brokers[1].port,
                                                     False, "PLAIN", "guest", "guest", "tcp")
        self.assertEqual(result.status, 0)

        # for each link, bridge the "fedX.fanout" exchanges:

        for _l in qmf.getObjects(_class="link"):
            # print("Link=%s:%s %s" % (_l.host, _l.port, str(_l.getBroker())))
            result = _l.bridge(False,  # durable
                                 "fedX.fanout",  # src
                                 "fedX.fanout",  # dst
                                 "",  # key
                                 "",  # tag
                                 "",  # excludes
                                 False, # srcIsQueue
                                 False, # srcIsLocal
                                 True,  # dynamic
                                 0)     # sync
            self.assertEqual(result.status, 0)

        # create a queue on B2, bound to the exchange
        self._brokers[2].client_session.queue_declare(queue="fedX1", exclusive=True, auto_delete=True)
        self._brokers[2].client_session.exchange_bind(queue="fedX1", exchange="fedX.fanout")

        # create a queue on B3, bound to the exchange
        self._brokers[3].client_session.queue_declare(queue="fedX1", exclusive=True, auto_delete=True)
        self._brokers[3].client_session.exchange_bind(queue="fedX1", exchange="fedX.fanout")

        # subscribe to messages arriving on B2's queue
        self.subscribe(self._brokers[2].client_session, queue="fedX1", destination="f1")
        queue_2 = self._brokers[2].client_session.incoming("f1")

        # subscribe to messages arriving on B3's queue
        self.subscribe(self._brokers[3].client_session, queue="fedX1", destination="f1")
        queue_3 = self._brokers[3].client_session.incoming("f1")

        # wait until the binding key has propagated to each broker (twice at
        # broker B1).  Work backwards from binding brokers.

        binding_counts = [1, 2, 1, 1]
        self.assertEqual(len(binding_counts), len(exchanges), "Update Test!")
        for i in range(3,-1,-1):
            retries = 0
            exchanges[i].update()
            while exchanges[i].bindingCount < binding_counts[i]:
                retries += 1
                self.failIfEqual(retries, 10,
                                 "binding failed to propagate to broker %d"
                                 % i)
                sleep(3)
                exchanges[i].update()

        # send 10 msgs from B0
        for i in range(1, 11):
            dp = self._brokers[0].client_session.delivery_properties()
            self._brokers[0].client_session.message_transfer(destination="fedX.fanout", message=Message(dp, "Message %d" % i))

        # wait for 10 messages to be forwarded from B0->B1,
        # 10 messages from B1->B2,
        # and 10 messages from B1->B3
        retries = 0
        for ex in exchanges:
            ex.update()
        while (exchanges[0].msgReceives != 10 or exchanges[0].msgRoutes != 10 or
               exchanges[1].msgReceives != 10 or exchanges[1].msgRoutes != 20 or
               exchanges[2].msgReceives != 10 or exchanges[2].msgRoutes != 10 or
               exchanges[3].msgReceives != 10 or exchanges[3].msgRoutes != 10):
            retries += 1
            self.failIfEqual(retries, 10,
                             "federation failed to route msgs %d:%d %d:%d %d:%d %d:%d"
                             % (exchanges[0].msgReceives,
                                exchanges[0].msgRoutes,
                                exchanges[1].msgReceives,
                                exchanges[1].msgRoutes,
                                exchanges[2].msgReceives,
                                exchanges[2].msgRoutes,
                                exchanges[3].msgReceives,
                                exchanges[3].msgRoutes))
            sleep(1)
            for ex in exchanges:
                ex.update()

        # get exactly 10 msgs on B2 and B3
        for i in range(1, 11):
            msg = queue_2.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)
            msg = queue_3.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)

        try:
            extra = queue_2.get(timeout=1)
            self.fail("Got unexpected message in queue_2: " + extra.body)
        except Empty: None

        try:
            extra = queue_3.get(timeout=1)
            self.fail("Got unexpected message in queue_3: " + extra.body)
        except Empty: None

        # tear down the queue on B2
        self._brokers[2].client_session.exchange_unbind(queue="fedX1", exchange="fedX.fanout")
        self._brokers[2].client_session.message_cancel(destination="f1")
        self._brokers[2].client_session.queue_delete(queue="fedX1")

        # @todo - find a proper way to check the propagation here!
        sleep(6)
        # wait for the binding count on B1 to drop from 2 to 1
        # retries = 0
        # exchanges[1].update()
        # while exchanges[1].bindingCount != 1:
        #     retries += 1
        #     self.failIfEqual(retries, 10,
        #                      "unbinding failed to propagate to broker B1: %d"
        #                      % exchanges[1].bindingCount)
        #     sleep(1)
        #     exchanges[1].update()

        # send 10 msgs from B0
        for i in range(1, 11):
            dp = self._brokers[0].client_session.delivery_properties()
            self._brokers[0].client_session.message_transfer(destination="fedX.fanout", message=Message(dp, "Message %d" % i))

        # verify messages are forwarded to B3 only
        # note: why exchanges[1].msgRoutes == 40???, not 20???  QPID-2499?
        retries = 0
        for ex in exchanges:
            ex.update()
        while (exchanges[0].msgReceives != 20 or exchanges[0].msgRoutes != 20 or
               exchanges[1].msgReceives != 20 or exchanges[1].msgRoutes != 40 or
               exchanges[2].msgReceives != 20 or exchanges[2].msgDrops != 10 or exchanges[2].msgRoutes != 10 or
               exchanges[3].msgReceives != 20 or exchanges[3].msgRoutes != 20):
            retries += 1
            self.failIfEqual(retries, 10,
                             "federation failed to route more msgs %d:%d %d:%d %d:%d %d:%d"
                             % (exchanges[0].msgReceives,
                                exchanges[0].msgRoutes,
                                exchanges[1].msgReceives,
                                exchanges[1].msgRoutes,
                                exchanges[2].msgReceives,
                                exchanges[2].msgRoutes,
                                exchanges[3].msgReceives,
                                exchanges[3].msgRoutes))
            sleep(1)
            for ex in exchanges:
                ex.update()

        # get exactly 10 msgs on B3 only
        for i in range(1, 11):
            msg = queue_3.get(timeout=5)
            self.assertEqual("Message %d" % i, msg.body)

        try:
            extra = queue_3.get(timeout=1)
            self.fail("Got unexpected message in queue_3: " + extra.body)
        except Empty: None

        # cleanup

        self._brokers[3].client_session.exchange_unbind(queue="fedX1", exchange="fedX.fanout")
        self._brokers[3].client_session.message_cancel(destination="f1")
        self._brokers[3].client_session.queue_delete(queue="fedX1")

        for _b in qmf.getObjects(_class="bridge"):
            result = _b.close()
            self.assertEqual(result.status, 0)

        for _l in qmf.getObjects(_class="link"):
            result = _l.close()
            self.assertEqual(result.status, 0)

        for _b in self._brokers:
            _b.client_session.exchange_delete(exchange="fedX.fanout")

        self._teardown_brokers()

        self.verify_cleanup()


    def getProperty(self, msg, name):
        for h in msg.headers:
            if hasattr(h, name): return getattr(h, name)
        return None            

    def getAppHeader(self, msg, name):
        headers = self.getProperty(msg, "application_headers")
        if headers:
            return headers[name]
        return None
