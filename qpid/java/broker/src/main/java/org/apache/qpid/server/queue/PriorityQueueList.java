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
package org.apache.qpid.server.queue;

import org.apache.qpid.server.message.ServerMessage;

public class PriorityQueueList implements QueueEntryList<SimpleQueueEntryImpl>
{
    private final AMQQueue _queue;
    private final PriorityQueueEntrySubList[] _priorityLists;
    private final int _priorities;
    private final int _priorityOffset;

    public PriorityQueueList(AMQQueue queue, int priorities)
    {
        _queue = queue;
        _priorityLists = new PriorityQueueEntrySubList[priorities];
        _priorities = priorities;
        _priorityOffset = 5-((priorities + 1)/2);
        for(int i = 0; i < priorities; i++)
        {
            _priorityLists[i] = new PriorityQueueEntrySubList(queue, i);
        }
    }

    public int getPriorities()
    {
        return _priorities;
    }

    public AMQQueue getQueue()
    {
        return _queue;
    }

    public SimpleQueueEntryImpl add(ServerMessage message)
    {
        int index = message.getMessageHeader().getPriority() - _priorityOffset;
        if(index >= _priorities)
        {
            index = _priorities-1;
        }
        else if(index < 0)
        {
            index = 0;
        }
        return _priorityLists[index].add(message);

    }

    public SimpleQueueEntryImpl next(SimpleQueueEntryImpl node)
    {
        SimpleQueueEntryImpl next = node.getNextValidEntry();

        if(next == null)
        {
            final QueueEntryList<?> nodeEntryList = node.getQueueEntryList();
            int index;
            for(index = _priorityLists.length-1; _priorityLists[index] != nodeEntryList; index--) {};

            while(next == null && index != 0)
            {
                index--;
                next = _priorityLists[index].getHead().getNextValidEntry();
            }

        }
        return next;
    }

    private final class PriorityQueueEntryListIterator implements QueueEntryIterator<SimpleQueueEntryImpl>
    {
        private final SimpleQueueEntryList.QueueEntryIteratorImpl[] _iterators = new SimpleQueueEntryList.QueueEntryIteratorImpl[ _priorityLists.length ];
        private SimpleQueueEntryImpl _lastNode;

        PriorityQueueEntryListIterator()
        {
            for(int i = 0; i < _priorityLists.length; i++)
            {
                _iterators[i] = _priorityLists[i].iterator();
            }
            _lastNode = _iterators[_iterators.length - 1].getNode();
        }


        public boolean atTail()
        {
            for(int i = 0; i < _iterators.length; i++)
            {
                if(!_iterators[i].atTail())
                {
                    return false;
                }
            }
            return true;
        }

        public SimpleQueueEntryImpl getNode()
        {
            return _lastNode;
        }

        public boolean advance()
        {
            for(int i = _iterators.length-1; i >= 0; i--)
            {
                if(_iterators[i].advance())
                {
                    _lastNode = _iterators[i].getNode();
                    return true;
                }
            }
            return false;
        }
    }

    public PriorityQueueEntryListIterator iterator()
    {
        return new PriorityQueueEntryListIterator();
    }

    public SimpleQueueEntryImpl getHead()
    {
        return _priorityLists[_priorities-1].getHead();
    }

    public void entryDeleted(final SimpleQueueEntryImpl queueEntry)
    {

    }

    static class Factory implements QueueEntryListFactory
    {
        private final int _priorities;

        Factory(int priorities)
        {
            _priorities = priorities;
        }

        public PriorityQueueList createQueueEntryList(AMQQueue queue)
        {
            return new PriorityQueueList(queue, _priorities);
        }
    }

    private static class PriorityQueueEntrySubList extends SimpleQueueEntryList
    {
        private int _listPriority;

        public PriorityQueueEntrySubList(AMQQueue queue, int listPriority)
        {
            super(queue);
            _listPriority = listPriority;
        }

        @Override
        protected PriorityQueueEntryImpl createQueueEntry(ServerMessage<?> message)
        {
            return new PriorityQueueEntryImpl(this, message);
        }

        public int getListPriority()
        {
            return _listPriority;
        }
    }

    private static class PriorityQueueEntryImpl extends SimpleQueueEntryImpl
    {
        public PriorityQueueEntryImpl(PriorityQueueEntrySubList queueEntryList, ServerMessage<?> message)
        {
            super(queueEntryList, message);
        }

        @Override
        public int compareTo(final QueueEntry o)
        {
            PriorityQueueEntrySubList pqel = (PriorityQueueEntrySubList)((PriorityQueueEntryImpl)o).getQueueEntryList();
            int otherPriority = pqel.getListPriority();
            int thisPriority = ((PriorityQueueEntrySubList) getQueueEntryList()).getListPriority();

            if(thisPriority != otherPriority)
            {
                /*
                 * Different priorities, so answer can only be greater than or less than
                 *
                 * A message with higher priority (e.g. 5) is conceptually 'earlier' in the
                 * priority queue than one with a lower priority (e.g. 4).
                 */
                return thisPriority > otherPriority ? -1 : 1;
            }
            else
            {
                return super.compareTo(o);
            }
        }
    }
}
