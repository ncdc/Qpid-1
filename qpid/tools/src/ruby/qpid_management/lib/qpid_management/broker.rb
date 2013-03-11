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

module Qpid
  module Management
    # Representation of the broker. Properties include:
    # - abandoned
    # - abandonedViaAlt
    # - acquires
    # - byteDepth
    # - byteFtdDepth
    # - byteFtdDequeues
    # - byteFtdEnqueues
    # - bytePersistDequeues
    # - bytePersistEnqueues
    # - byteTotalDequeues
    # - byteTotalEnqueues
    # - byteTxnDequeues
    # - byteTxnEnqueues
    # - connBacklog
    # - dataDir
    # - discardsLvq
    # - discardsNoRoute
    # - discardsOverflow
    # - discardsPurge
    # - discardsRing
    # - discardsSubscriber
    # - discardsTtl
    # - maxConns
    # - mgmtPubInterval
    # - mgmtPublish
    # - msgDepth
    # - msgFtdDepth
    # - msgFtdDequeues
    # - msgFtdEnqueues
    # - msgPersistDequeues
    # - msgPersistEnqueues
    # - msgTotalDequeues
    # - msgTotalEnqueues
    # - msgTxnDequeues
    # - msgTxnEnqueues
    # - name
    # - port
    # - queueCount
    # - releases
    # - reroutes
    # - stagingThreshold
    # - systemRef
    # - uptime
    # - version
    # - workerThreads
    class Broker < BrokerObject
      # Queries the broker for all the Connection QMF objects.
      # @return [Array<Connection>] an Array of Connection QMF objects
      def connections()
        @agent.find_all_by_class(Connection)
      end

      # Queries the broker for a single Connection QMF object.
      # @param [String] object id of the connection, e.g. "[::1]:5672-[::1]:41066". Note, this is the short form of the object id; "org.apache.qpid.broker:connection:" is prepended to this parameter.
      # @return [Connection] the Connection QMF object
      def connection(oid)
        @agent.find_by_object_id(Connection, "org.apache.qpid.broker:connection:#{oid}")
      end

      # Queries the broker for all the Session QMF objects.
      # @return [Array<Session>] an Array of Session QMF objects
      def sessions
        @agent.find_all_by_class(Session)
      end

      # Queries the broker for a single Session QMF object.
      # @param [String] object id of the session, e.g. "d1155105-b2c6-4b9e-8f1b-08fba0f05bf2". Note, this is the short form of the object id; "org.apache.qpid.broker:session:" is prepended to this parameter.
      # @return [Session] the Session QMF object
      def session(oid)
        @agent.find_by_object_id(Session, "org.apache.qpid.broker:session:#{oid}")
      end

      # Queries the broker for all the Subscription QMF objects.
      # @return [Array<Subscription>] an Array of Subscription QMF objects
      def subscriptions
        @agent.find_all_by_class(Subscription)
      end

      # Queries the broker for a single Subscription QMF object.
      # @param [String] object id of the subscription, e.g. "org.apache.qpid.broker:session:d1155105-b2c6-4b9e-8f1b-08fba0f05bf2,org.apache.qpid.broker:queue:amq.direct_1b5550ea-3fcd-41a5-b89a-de58edf840a5,amq.direct". Note, this is the short form of the object id; "org.apache.qpid.broker:subscription:" is prepended to this parameter.
      # @return [Subscription] the Subscription QMF object
      def subscription(oid)
        @agent.find_by_object_id(Subscription, "org.apache.qpid.broker:subscription:#{oid}")
      end

      # Queries the broker for all the Exchange QMF objects.
      # @return [Array<Exchange>] an Array of Exchange QMF objects
      def exchanges
        @agent.find_all_by_class(Exchange)
      end

      # Queries the broker for a single Exchange QMF object.
      # @param [String] object id of the exchange, e.g. "amq.direct". Note, this is the short form of the object id; "org.apache.qpid.broker:exchange:" is prepended to this parameter.
      # @return [Exchange] the Exchange QMF object
      def exchange(name)
        @agent.find_by_object_id(Exchange, "org.apache.qpid.broker:exchange:#{name}")
      end

      # Queries the broker for all the Queue QMF objects.
      # @return [Array<Queue>] an Array of Queue QMF objects
      def queues
        @agent.find_all_by_class(Queue)
      end

      # Queries the broker for a single Queue QMF object.
      # @param [String] object id of the queue, e.g. "myqueue". Note, this is the short form of the object id; "org.apache.qpid.broker:queue:" is prepended to this parameter.
      # @return [Queue] the Queue QMF object
      def queue(name)
        @agent.find_by_object_id(Queue, "org.apache.qpid.broker:queue:#{name}")
      end

      # Queries the broker for all the Binding QMF objects.
      # @return [Array<Binding>] an Array of Binding QMF objects
      def bindings
        @agent.find_all_by_class(Binding)
      end

      # Queries the broker for a single Binding QMF object.
      # @param [String] object id of the binding, e.g. "org.apache.qpid.broker:exchange:amq.topic,org.apache.qpid.broker:queue:myqueue,foo.#.bar". Note, this is the short form of the object id; "org.apache.qpid.broker:binding:" is prepended to this parameter.
      # @return [Binding] the Binding QMF object
      def binding(name)
        @agent.find_by_object_id(Binding, "org.apache.qpid.broker:binding:#{name}")
      end

      # Queries the broker for all the Link QMF objects.
      # @return [Array<Link>] an Array of Link QMF objects
      def links
        @agent.find_all_by_class(Link)
      end

      # Queries the broker for a single Link QMF object.
      # @param [String] object id of the link, e.g. "qpid.tcp:10.0.0.1:8888". Note, this is the short form of the object id; "org.apache.qpid.broker:link:" is prepended to this parameter.
      # @return [Link] the Link QMF object
      def link(name)
        @agent.find_by_object_id(Link, "org.apache.qpid.broker:link:#{name}")
      end

      # Queries the broker for all the Bridge QMF objects.
      # @return [Array<Bridge>] an Array of Bridge QMF objects
      def bridges
        @agent.find_all_by_class(Bridge)
      end

      # Queries the broker for a single Bridge QMF object.
      # @param [String] object id of the bridge, e.g. "org.apache.qpid.broker:link:qpid.tcp:10.0.0.1:8888,qpid.tcp:10.0.0.1:8888!bridgeq!amq.topic!". Note, this is the short form of the object id; "org.apache.qpid.broker:bridge:" is prepended to this parameter.
      # @return [Bridge] the Bridge QMF object
      def bridge(name)
        @agent.find_by_object_id(Bridge, "org.apache.qpid.broker:bridge:#{name}")
      end

      # Queries the broker for the ACL QMF object.
      # @return [Acl] the ACL QMF object
      def acl
        @agent.find_first_by_class(Acl)
      end

      # Queries the broker for the Memory QMF object.
      # @return [Memory] the Memory QMF object
      def memory
        @agent.find_first_by_class(Memory)
      end
      # Adds an exchange to the broker
      # @param [String] type exchange type (fanout, direct, topic, headers, xml)
      # @param [String] name exchange name
      # @param [Hash] options exchange creation options
      def add_exchange(type, name, options={})
        create_broker_object('exchange', name, options.merge!({'exchange-type' => type}))
      end

      # Deletes an exchange from the broekr
      # @param [String] name exchange name
      def delete_exchange(name)
        invoke_method('delete', {'type' => 'exchange', 'name' => name})
      end

      # Adds a queue to the broker
      # @param [String] name queue name
      # @param [Hash] options queue creation options
      def add_queue(name, options={})
        create_broker_object('queue', name, options)
      end

      # Deletes a queue from the broker
      # @param [String] name queue name
      def delete_queue(name)
        invoke_method('delete', {'type' => 'queue', 'name' => name})
      end

      # Adds a binding from an exchange to a queue
      # @param [String] exchange exchange name
      # @param [String] queue queue name
      # @param [String] key binding key
      # @param [Hash] options binding creation options
      def add_binding(exchange, queue, key="", options={})
        create_broker_object('binding', "#{exchange}/#{queue}/#{key}", options)
      end

      # Deletes a binding from an exchange to a queue
      # @param [String] exchange exchange name
      # @param [String] queue queue name
      # @param [String] key binding key
      def delete_binding(exchange, queue, key="")
        invoke_method('delete', {'type' => 'binding', 'name' => "#{exchange}/#{queue}/#{key}"})
      end

      # Adds a link to a remote broker
      # @param [String] name link name
      # @param [String] host remote broker host name or IP address
      # @param [Fixnum] port remote broker port
      # @param [String] transport transport mechanism used to connect to the remote broker
      # @param [Boolean] durable should this link be persistent
      # @param [String] auth_mechanism authentication mechanism to use
      # @param [String] username user name to authenticate with the remote broker
      # @param [String] password password for the user name
      def add_link(name, host, port, transport='tcp', durable=false, auth_mechanism="", username="", password="")
        options = {
          'host' => host,
          'port' => port,
          'transport' => transport,
          'durable' => durable,
          'authMechanism' => auth_mechanism,
          'username' => username,
          'password' => password
        }

        create_broker_object('link', name, options)
      end

      # Deletes a link to a remote broker
      # @param [String] name link name
      def delete_link(name)
        invoke_method('delete', {'type' => 'link', 'name' => name})
      end

      # Adds a queue route
      # @param [String] name the name of the bridge to create
      # @param [String] link the name of the link to use
      # @param [String] queue the name of the source queue from which messages are pulled
      # @param [String] exchange the name of the destination exchange to which messages are sent
      # @param [Fixnum] sync the number of messages to send before issuing an explicit session sync
      def add_queue_route(name, link, queue, exchange, sync)
        properties = {
          'link' => link,
          'src' => queue,
          'dest' => exchange,
          'srcIsQueue' => true,
          'sync' => sync
        }
        create_broker_object('bridge', name, properties)
      end

      # Deletes a bridge (route)
      # @param [String] name bridge name
      def delete_bridge(name)
        invoke_method('delete', {'type' => 'bridge', 'name' => name})
      end

    private

      def create_broker_object(type, name, options)
        invoke_method('create', {'type' => type,
                                 'name' => name,
                                 'properties' => options,
                                 'strict' => true})
      end
    end
  end
end
