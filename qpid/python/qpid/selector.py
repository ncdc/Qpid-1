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
import atexit, os, time
from compat import select, set
from threading import Thread, Lock

class Acceptor:

  def __init__(self, sock, handler):
    self.sock = sock
    self.handler = handler

  def fileno(self):
    return self.sock.fileno()

  def reading(self):
    return True

  def writing(self):
    return False

  def readable(self):
    sock, addr = self.sock.accept()
    self.handler(sock)

class Sink:

  def __init__(self, fd):
    self.fd = fd

  def fileno(self):
    return self.fd

  def reading(self):
    return True

  def readable(self):
    os.read(self.fd, 65536)

  def __repr__(self):
    return "Sink(%r)" % self.fd

class Selector:

  lock = Lock()
  DEFAULT = None

  @staticmethod
  def default():
    Selector.lock.acquire()
    try:
      if Selector.DEFAULT is None:
        sel = Selector()
        atexit.register(sel.stop)
        sel.start()
        Selector.DEFAULT = sel
      return Selector.DEFAULT
    finally:
      Selector.lock.release()

  def __init__(self):
    self.selectables = set()
    self.reading = set()
    self.writing = set()
    self.wait_fd, self.wakeup_fd = os.pipe()
    self.reading.add(Sink(self.wait_fd))
    self.stopped = False
    self.thread = None

  def wakeup(self):
    os.write(self.wakeup_fd, "\0")

  def register(self, selectable):
    self.selectables.add(selectable)
    self.modify(selectable)

  def _update(self, selectable):
    if selectable.reading():
      self.reading.add(selectable)
    else:
      self.reading.discard(selectable)
    if selectable.writing():
      self.writing.add(selectable)
    else:
      self.writing.discard(selectable)
    return selectable.timing()

  def modify(self, selectable):
    self._update(selectable)
    self.wakeup()

  def unregister(self, selectable):
    self.reading.discard(selectable)
    self.writing.discard(selectable)
    self.selectables.discard(selectable)
    self.wakeup()

  def start(self):
    self.stopped = False
    self.thread = Thread(target=self.run)
    self.thread.setDaemon(True)
    self.thread.start();

  def run(self):
    while not self.stopped:
      wakeup = None
      for sel in self.selectables.copy():
        t = self._update(sel)
        if t is not None:
          if wakeup is None:
            wakeup = t
          else:
            wakeup = min(wakeup, t)

      if wakeup is None:
        timeout = None
      else:
        timeout = max(0, wakeup - time.time())

      rd, wr, ex = select(self.reading, self.writing, (), timeout)

      for sel in wr:
        sel.writeable()

      for sel in rd:
        sel.readable()

      now = time.time()
      for sel in self.selectables.copy():
        w = sel.timing()
        if w is not None and now > w:
          sel.timeout()

  def stop(self, timeout=None):
    self.stopped = True
    self.wakeup()
    self.thread.join(timeout)
    self.thread = None
