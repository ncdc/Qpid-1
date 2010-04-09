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

import time
from qpid.harness import Skipped
from qpid.messaging import *
from qpid.tests import Test

class Base(Test):

  def setup_connection(self):
    return None

  def setup_session(self):
    return None

  def setup_sender(self):
    return None

  def setup_receiver(self):
    return None

  def setup(self):
    self.test_id = uuid4()
    self.broker = self.config.broker
    try:
      self.conn = self.setup_connection()
    except ConnectError, e:
      raise Skipped(e)
    self.ssn = self.setup_session()
    self.snd = self.setup_sender()
    if self.snd is not None:
      self.snd.durable = self.durable()
    self.rcv = self.setup_receiver()

  def teardown(self):
    if self.conn is not None and self.conn.attached():
      self.conn.close()

  def content(self, base, count = None):
    if count is None:
      return "%s[%s]" % (base, self.test_id)
    else:
      return "%s[%s, %s]" % (base, count, self.test_id)

  def message(self, base, count = None, **kwargs):
    return Message(content=self.content(base, count), **kwargs)

  def ping(self, ssn):
    PING_Q = 'ping-queue; {create: always, delete: always}'
    # send a message
    sender = ssn.sender(PING_Q, durable=self.durable())
    content = self.content("ping")
    sender.send(content)
    receiver = ssn.receiver(PING_Q)
    msg = receiver.fetch(0)
    ssn.acknowledge()
    assert msg.content == content, "expected %r, got %r" % (content, msg.content)

  def drain(self, rcv, limit=None, timeout=0, expected=None, redelivered=False):
    messages = []
    try:
      while limit is None or len(messages) < limit:
        messages.append(rcv.fetch(timeout=timeout))
    except Empty:
      pass
    if expected is not None:
      self.assertEchos(expected, messages, redelivered)
    return messages

  def diff(self, m1, m2):
    result = {}
    for attr in ("id", "subject", "user_id", "to", "reply_to",
                 "correlation_id", "durable", "priority", "ttl",
                 "redelivered", "properties", "content_type",
                 "content"):
      a1 = getattr(m1, attr)
      a2 = getattr(m2, attr)
      if a1 != a2:
        result[attr] = (a1, a2)
    return result

  def assertEcho(self, msg, echo, redelivered=False):
    if not isinstance(msg, Message) or not isinstance(echo, Message):
      if isinstance(msg, Message):
        msg = msg.content
      if isinstance(echo, Message):
        echo = echo.content
        assert msg == echo, "expected %s, got %s" % (msg, echo)
    else:
      delta = self.diff(msg, echo)
      mttl, ettl = delta.pop("ttl", (0, 0))
      if redelivered:
        assert echo.redelivered, \
            "expected %s to be redelivered: %s" % (msg, echo)
        if delta.has_key("redelivered"):
          del delta["redelivered"]
      assert mttl is not None and ettl is not None, "%s, %s" % (mttl, ettl)
      assert mttl >= ettl, "%s, %s" % (mttl, ettl)
      assert not delta, "expected %s, got %s, delta %s" % (msg, echo, delta)

  def assertEchos(self, msgs, echoes, redelivered=False):
    assert len(msgs) == len(echoes), "%s, %s" % (msgs, echoes)
    for m, e in zip(msgs, echoes):
      self.assertEcho(m, e, redelivered)

  def assertEmpty(self, rcv):
    contents = self.drain(rcv)
    assert len(contents) == 0, "%s is supposed to be empty: %s" % (rcv, contents)

  def assertPending(self, rcv, expected):
    p = rcv.pending()
    assert p == expected, "expected %s, got %s" % (expected, p)

  def sleep(self):
    time.sleep(self.delay())

  def delay(self):
    return float(self.config.defines.get("delay", "2"))

  def get_bool(self, name):
    return self.config.defines.get(name, "false").lower() in ("true", "yes", "1")

  def durable(self):
    return self.get_bool("durable")

  def reconnect(self):
    return self.get_bool("reconnect")


  def transport(self):
    if self.broker.scheme == self.broker.AMQPS:
      return "ssl"
    else:
      return "tcp"

  def connection_options(self):
    return {"reconnect": self.reconnect(),
            "transport": self.transport()}

import address, endpoints, message
