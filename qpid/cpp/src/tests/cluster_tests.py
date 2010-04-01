#!/usr/bin/env python

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

import os, signal, sys, time, imp, re
from qpid import datatypes, messaging
from qpid.brokertest import *
from qpid.harness import Skipped
from qpid.messaging import Message
from threading import Thread
from logging import getLogger
from itertools import chain

log = getLogger("qpid.cluster_tests")

# Note: brokers that shut themselves down due to critical error during
# normal operation will still have an exit code of 0. Brokers that
# shut down because of an error found during initialize will exit with
# a non-0 code. Hence the apparently inconsistent use of EXPECT_EXIT_OK
# and EXPECT_EXIT_FAIL in some of the tests below.

# FIXME aconway 2010-03-11: resolve this - ideally any exit due to an error
# should give non-0 exit status.

# Import scripts as modules
qpid_cluster=import_script(checkenv("QPID_CLUSTER_EXEC"))


def readfile(filename):
    """Returns te content of file named filename as a string"""
    f = file(filename)
    try: return f.read()
    finally: f.close()

class ShortTests(BrokerTest):
    """Short cluster functionality tests."""

    def test_message_replication(self):
        """Test basic cluster message replication."""
        # Start a cluster, send some messages to member 0.
        cluster = self.cluster(2)
        s0 = cluster[0].connect().session()
        s0.sender("q; {create:always}").send(Message("x"))
        s0.sender("q; {create:always}").send(Message("y"))
        s0.connection.close()

        # Verify messages available on member 1.
        s1 = cluster[1].connect().session()
        m = s1.receiver("q", capacity=1).fetch(timeout=1)
        s1.acknowledge()
        self.assertEqual("x", m.content)
        s1.connection.close()

        # Start member 2 and verify messages available.
        s2 = cluster.start().connect().session()
        m = s2.receiver("q", capacity=1).fetch(timeout=1)
        s2.acknowledge()
        self.assertEqual("y", m.content)
        s2.connection.close()

    def test_store_direct_update_match(self):
        """Verify that brokers stores an identical message whether they receive it
        direct from clients or during an update, no header or other differences"""
        cluster = self.cluster(0, args=["--load-module", self.test_store_lib])
        cluster.start(args=["--test-store-dump", "direct.dump"])
        # Try messages with various headers
        cluster[0].send_message("q", Message(durable=True, content="foobar",
                                             subject="subject",
                                             reply_to="reply_to",
                                             properties={"n":10}))
        # Try messages of different sizes
        for size in range(0,10000,100):
            cluster[0].send_message("q", Message(content="x"*size, durable=True))
        # Try sending via named exchange
        c = cluster[0].connect_old()
        s = c.session(str(qpid.datatypes.uuid4()))
        s.exchange_bind(exchange="amq.direct", binding_key="foo", queue="q")
        props = s.delivery_properties(routing_key="foo", delivery_mode=2)
        s.message_transfer(
            destination="amq.direct",
            message=qpid.datatypes.Message(props, "content"))

        # Now update a new member and compare their dumps.
        cluster.start(args=["--test-store-dump", "updatee.dump"])
        assert readfile("direct.dump") == readfile("updatee.dump")
        os.remove("direct.dump")
        os.remove("updatee.dump")
        
class LongTests(BrokerTest):
    """Tests that can run for a long time if -DDURATION=<minutes> is set"""
    def duration(self):
        d = self.config.defines.get("DURATION")
        if d: return float(d)*60
        else: return 3                  # Default is to be quick

    def test_failover(self):
        """Test fail-over during continuous send-receive with errors"""

        # Original cluster will all be killed so expect exit with failure
        cluster = self.cluster(3, expect=EXPECT_EXIT_FAIL)
        for b in cluster: ErrorGenerator(b)

        # Start sender and receiver threads
        cluster[0].declare_queue("test-queue")
        sender = NumberedSender(cluster[1], 1000) # Max queue depth
        receiver = NumberedReceiver(cluster[2], sender)
        receiver.start()
        sender.start()

        # Kill original brokers, start new ones for the duration.
        endtime = time.time() + self.duration()
        i = 0
        while time.time() < endtime:
            cluster[i].kill()
            i += 1
            b = cluster.start(expect=EXPECT_EXIT_FAIL)
            ErrorGenerator(b)
            time.sleep(min(5,self.duration()/2))
        sender.stop()
        receiver.stop()
        for i in range(i, len(cluster)): cluster[i].kill()

    def test_management(self):
        """Stress test: Run management clients and other clients concurrently."""

        # TODO aconway 2010-03-03: move to brokertest framework
        class ClientLoop(StoppableThread):
            """Run a client executable in a loop."""
            def __init__(self, broker, cmd):
                StoppableThread.__init__(self)
                self.broker=broker
                self.cmd = cmd          # Client command.
                self.lock = Lock()
                self.process = None     # Client process.
                self._expect_fail = False
                self.start()

            def run(self):
                try:
                    while True:
                        self.lock.acquire()
                        try:
                            if self.stopped: break
                            self.process = self.broker.test.popen(
                                self.cmd, expect=EXPECT_UNKNOWN)
                        finally: self.lock.release()
                        try: exit = self.process.wait()
                        except OSError, e:
                            # Seems to be a race in wait(), it throws
                            # "no such process" during test shutdown.
                            # Doesn't indicate a test error, ignore.
                            return
                        except Exception, e:
                            self.process.unexpected(
                                "client of %s: %s"%(self.broker.name, e))
                        self.lock.acquire()
                        try:
                            # Quit and ignore errors if stopped or expecting failure.
                            if self.stopped: break
                            if not self._expect_fail and exit != 0:
                                self.process.unexpected(
                                    "client of %s exit code %s"%(self.broker.name, exit))
                        finally: self.lock.release()
                except Exception, e:
                    self.error = RethrownException("Error in ClientLoop.run")

            def expect_fail(self):
                """Ignore exit status of the currently running client."""
                self.lock.acquire()
                self._expect_fail = True
                self.lock.release()
                
            def stop(self):
                """Stop the running client and wait for it to exit"""
                self.lock.acquire()
                if self.stopped: return
                try:
                    self.stopped = True
                    if self.process:
                        try: self.process.kill() # Kill the client.
                        except OSError: pass # The client might not be running.
                finally: self.lock.release()
                StoppableThread.stop(self)

        # def test_management
        args=["--mgmt-pub-interval", 1] # Publish management information every second.
        # Use store if present.
        if BrokerTest.store_lib: args +=["--load-module", BrokerTest.store_lib]
        cluster = self.cluster(3, args)
        
        clients = [] # Ordinary clients that only connect to one broker.
        mclients = [] # Management clients that connect to every broker in the cluster.

        def start_clients(broker, i):
            """Start ordinary clients for a broker. Start one client per broker.
            Round-robin on a colllection of different clients."""
            cmds=[
                ["perftest", "--count", 50000,
                 "--base-name", str(qpid.datatypes.uuid4()), "--port", broker.port()],
                ["qpid-queue-stats", "-a", "localhost:%s" %(broker.port())],
                ["testagent", "localhost", str(broker.port())] ]
            clients.append([ClientLoop(broker, cmds[i%len(cmds)])])

        def start_mclients(broker):
            """Start management clients that make multiple connections."""
            cmd = ["qpid-stat", "-b", "localhost:%s" %(broker.port())]
            mclients.append(ClientLoop(broker, cmd))

        endtime = time.time() + self.duration()
        alive = 0                       # First live cluster member
        for i in range(len(cluster)):
            start_clients(cluster[i], i)
        start_mclients(cluster[alive])

        while time.time() < endtime:
            time.sleep(min(5,self.duration()/2))
            for b in cluster[alive:]: b.ready() # Check if a broker crashed.
            # Kill the first broker, expect the clients to fail. 
            for c in clients[alive] + mclients: c.expect_fail()
            b = cluster[alive]
            b.expect = EXPECT_EXIT_FAIL
            b.kill()
            # Stop the brokers clients and all the mclients.
            for c in clients[alive] + mclients: c.stop()
            clients[alive] = []
            mclients = []
            # Start another broker and clients
            alive += 1
            cluster.start()
            start_clients(cluster[-1], len(cluster)-1)
            start_mclients(cluster[alive])
        for c in chain(mclients, *clients):
            c.stop()

class StoreTests(BrokerTest):
    """
    Cluster tests that can only be run if there is a store available.
    """
    def args(self):
        assert BrokerTest.store_lib 
        return ["--load-module", BrokerTest.store_lib]

    def test_store_loaded(self):
        """Ensure we are indeed loading a working store"""
        broker = self.broker(self.args(), name="recoverme", expect=EXPECT_EXIT_FAIL)
        m = Message("x", durable=True)
        broker.send_message("q", m)
        broker.kill()
        broker = self.broker(self.args(), name="recoverme")
        self.assertEqual("x", broker.get_message("q").content)

    def test_kill_restart(self):
        """Verify we can kill/resetart a broker with store in a cluster"""
        cluster = self.cluster(1, self.args())
        cluster.start("restartme", expect=EXPECT_EXIT_FAIL).kill()

        # Send a message, retrieve from the restarted broker
        cluster[0].send_message("q", "x")
        m = cluster.start("restartme").get_message("q")
        self.assertEqual("x", m.content)

    def stop_cluster(self,broker):
        """Clean shut-down of a cluster"""
        self.assertEqual(0, qpid_cluster.main(
            ["qpid-cluster", "-kf", broker.host_port()]))

    def test_persistent_restart(self):
        """Verify persistent cluster shutdown/restart scenarios"""
        cluster = self.cluster(0, args=self.args() + ["--cluster-size=3"])
        a = cluster.start("a", expect=EXPECT_EXIT_OK, wait=False)
        b = cluster.start("b", expect=EXPECT_EXIT_OK, wait=False)
        c = cluster.start("c", expect=EXPECT_EXIT_FAIL, wait=True)
        a.send_message("q", Message("1", durable=True))
        # Kill & restart one member.
        c.kill()
        self.assertEqual(a.get_message("q").content, "1")
        a.send_message("q", Message("2", durable=True))
        c = cluster.start("c", expect=EXPECT_EXIT_OK)
        self.assertEqual(c.get_message("q").content, "2")
        # Shut down the entire cluster cleanly and bring it back up
        a.send_message("q", Message("3", durable=True))
        self.stop_cluster(a)
        a = cluster.start("a", wait=False)
        b = cluster.start("b", wait=False)
        c = cluster.start("c", wait=True)
        self.assertEqual(a.get_message("q").content, "3")

    def test_persistent_partial_failure(self):
        # Kill 2 members, shut down the last cleanly then restart
        # Ensure we use the clean database
        cluster = self.cluster(0, args=self.args() + ["--cluster-size=3"])
        a = cluster.start("a", expect=EXPECT_EXIT_FAIL, wait=False)
        b = cluster.start("b", expect=EXPECT_EXIT_FAIL, wait=False)
        c = cluster.start("c", expect=EXPECT_EXIT_OK, wait=True)
        a.send_message("q", Message("4", durable=True))
        a.kill()
        b.kill()
        self.assertEqual(c.get_message("q").content, "4")
        c.send_message("q", Message("clean", durable=True))
        self.stop_cluster(c)
        a = cluster.start("a", wait=False)
        b = cluster.start("b", wait=False)
        c = cluster.start("c", wait=True)
        self.assertEqual(a.get_message("q").content, "clean")
        
    def test_wrong_cluster_id(self):
        # Start a cluster1 broker, then try to restart in cluster2
        cluster1 = self.cluster(0, args=self.args())
        a = cluster1.start("a", expect=EXPECT_EXIT_OK)
        a.terminate()
        cluster2 = self.cluster(1, args=self.args())
        try:
            a = cluster2.start("a", expect=EXPECT_EXIT_FAIL)
            a.ready()
            self.fail("Expected exception")
        except: pass

    def test_wrong_shutdown_id(self):
        # Start 2 members and shut down.
        cluster = self.cluster(0, args=self.args()+["--cluster-size=2"])
        a = cluster.start("a", expect=EXPECT_EXIT_OK, wait=False)
        b = cluster.start("b", expect=EXPECT_EXIT_OK, wait=False)
        self.stop_cluster(a)
        self.assertEqual(a.wait(), 0)
        self.assertEqual(b.wait(), 0)

        # Restart with a different member and shut down.
        a = cluster.start("a", expect=EXPECT_EXIT_OK, wait=False)
        c = cluster.start("c", expect=EXPECT_EXIT_OK, wait=False)
        self.stop_cluster(a)
        self.assertEqual(a.wait(), 0)
        self.assertEqual(c.wait(), 0)
        # Mix members from both shutdown events, they should fail
        # FIXME aconway 2010-03-11: can't predict the exit status of these
        # as it depends on the order of delivery of initial-status messages.
        # See comment at top of this file.
        a = cluster.start("a", expect=EXPECT_UNKNOWN, wait=False)
        b = cluster.start("b", expect=EXPECT_UNKNOWN, wait=False)
        self.assertRaises(Exception, lambda: a.ready())
        self.assertRaises(Exception, lambda: b.ready())

    def test_solo_store_clean(self):
        # A single node cluster should always leave a clean store.
        cluster = self.cluster(0, self.args())
        a = cluster.start("a", expect=EXPECT_EXIT_FAIL)
        a.send_message("q", Message("x", durable=True))
        a.kill()
        a = cluster.start("a")
        self.assertEqual(a.get_message("q").content, "x")

    def test_last_store_clean(self):
        # Verify that only the last node in a cluster to shut down has
        # a clean store. Start with cluster of 3, reduce to 1 then
        # increase again to ensure that a node that was once alone but
        # finally did not finish as the last node does not get a clean
        # store.
        cluster = self.cluster(0, self.args())
        a = cluster.start("a", expect=EXPECT_EXIT_FAIL)
        self.assertEqual(a.store_state(), "clean")
        b = cluster.start("b", expect=EXPECT_EXIT_FAIL)
        c = cluster.start("c", expect=EXPECT_EXIT_FAIL)
        self.assertEqual(b.store_state(), "dirty")
        self.assertEqual(c.store_state(), "dirty")
        retry(lambda: a.store_state() == "dirty") 

        a.send_message("q", Message("x", durable=True))
        a.kill()
        b.kill()                # c is last man, will mark store clean
        retry(lambda: c.store_state() == "clean") 
        a = cluster.start("a", expect=EXPECT_EXIT_FAIL) # c no longer last man
        retry(lambda: c.store_state() == "dirty") 
        c.kill()                        # a is now last man
        retry(lambda: a.store_state() == "clean") 
        a.kill()
        self.assertEqual(a.store_state(), "clean")
        self.assertEqual(b.store_state(), "dirty")
        self.assertEqual(c.store_state(), "dirty")

    def test_restart_clean(self):
        """Verify that we can re-start brokers one by one in a
        persistent cluster after a clean oshutdown"""
        cluster = self.cluster(0, self.args())
        a = cluster.start("a", expect=EXPECT_EXIT_OK)
        b = cluster.start("b", expect=EXPECT_EXIT_OK)
        c = cluster.start("c", expect=EXPECT_EXIT_OK)
        a.send_message("q", Message("x", durable=True))
        self.stop_cluster(a)
        a = cluster.start("a")
        b = cluster.start("b")
        c = cluster.start("c")
        self.assertEqual(c.get_message("q").content, "x")

    def test_join_sub_size(self):
        """Verify that after starting a cluster with cluster-size=N,
        we can join new members even if size < N-1"""
        cluster = self.cluster(0, self.args()+["--cluster-size=3"])
        a = cluster.start("a", wait=False, expect=EXPECT_EXIT_FAIL)
        b = cluster.start("b", wait=False, expect=EXPECT_EXIT_FAIL)
        c = cluster.start("c")
        a.send_message("q", Message("x", durable=True))
        a.send_message("q", Message("y", durable=True))
        a.kill()
        b.kill()
        a = cluster.start("a")
        self.assertEqual(c.get_message("q").content, "x")
        b = cluster.start("b")
        self.assertEqual(c.get_message("q").content, "y")

