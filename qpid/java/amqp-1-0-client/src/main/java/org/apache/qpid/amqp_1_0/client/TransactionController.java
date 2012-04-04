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

package org.apache.qpid.amqp_1_0.client;

import org.apache.qpid.amqp_1_0.messaging.SectionEncoder;
import org.apache.qpid.amqp_1_0.transport.DeliveryStateHandler;
import org.apache.qpid.amqp_1_0.transport.SendingLinkEndpoint;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.DeliveryState;
import org.apache.qpid.amqp_1_0.type.messaging.AmqpValue;
import org.apache.qpid.amqp_1_0.type.transaction.Declare;
import org.apache.qpid.amqp_1_0.type.transaction.Declared;
import org.apache.qpid.amqp_1_0.type.transaction.Discharge;
import org.apache.qpid.amqp_1_0.type.transport.Transfer;


public class TransactionController implements DeliveryStateHandler
{
    private static final Binary DELIVERY_TAG = new Binary(new byte[]{(byte) 0});
    private SendingLinkEndpoint _endpoint;
    private Session _session;
    private volatile DeliveryState _state;
    private boolean _received;

    public TransactionController(Session session, SendingLinkEndpoint tcLinkEndpoint)
    {
        _session = session;
        _endpoint = tcLinkEndpoint;
        _endpoint.setDeliveryStateHandler(this);
    }

    public Transaction beginTransaction()
    {


        Binary txnId = declare();
        return new Transaction(this, txnId);
    }

    private Binary declare()
    {
        SectionEncoder encoder = _session.getSectionEncoder();


        AmqpValue section = new AmqpValue(new Declare());


        Transfer transfer = new Transfer();
        transfer.setPayload(section.encode(encoder).asByteBuffer());
        transfer.setDeliveryTag(DELIVERY_TAG);
        transfer.setSettled(Boolean.FALSE);
        final Object lock = _endpoint.getLock();
        synchronized(lock)
        {
            while(!_endpoint.hasCreditToSend())
            {
                try
                {
                    lock.wait();
                }
                catch (InterruptedException e)
                {

                }
            }
            _state = null;
            _received = false;
            _endpoint.transfer(transfer);

            //TODO - rationalise sending of flows
            // _endpoint.sendFlow();
        }
        synchronized (this)
        {
            while(!_received)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException e)
                {

                }
            }
        }


        return ((Declared) _state).getTxnId();
    }


    public void commit(final Transaction transaction)
    {
        discharge(transaction.getTxnId(), false);
    }

    public void rollback(final Transaction transaction)
    {
        discharge(transaction.getTxnId(), true);
    }

    private void discharge(final Binary txnId, final boolean fail)
    {
        Discharge discharge = new Discharge();
        discharge.setTxnId(txnId);
        discharge.setFail(fail);
        SectionEncoder encoder = _session.getSectionEncoder();


        AmqpValue section = new AmqpValue(discharge);

        Transfer transfer = new Transfer();
        transfer.setPayload(section.encode(encoder).asByteBuffer());
        transfer.setDeliveryTag(DELIVERY_TAG);
        transfer.setSettled(Boolean.FALSE);

        final Object lock = _endpoint.getLock();
        synchronized(lock)
        {
            while(!_endpoint.hasCreditToSend())
            {
                try
                {
                    lock.wait();
                }
                catch (InterruptedException e)
                {

                }
            }
            _state = null;
            _received = false;
            _endpoint.transfer(transfer);

            //TODO - rationalise sending of flows
            // _endpoint.sendFlow();
        }
        synchronized (this)
        {
            while(!_received)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException e)
                {

                }
            }
        }


    }

    public void handle(final Binary deliveryTag, final DeliveryState state, final Boolean settled)
    {
        synchronized(this)
        {
            _state = state;
            _received = true;

            if(!Boolean.TRUE.equals(settled))
            {
                _endpoint.updateDisposition(deliveryTag, state, true);
            }

            notifyAll();
        }
    }

    public void close()
    {
        _endpoint.close();
    }
}
