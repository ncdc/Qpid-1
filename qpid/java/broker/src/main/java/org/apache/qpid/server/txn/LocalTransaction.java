package org.apache.qpid.server.txn;

import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.message.EnqueableMessage;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.store.TransactionLog;
import org.apache.qpid.AMQException;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

public class LocalTransaction implements ServerTransaction
{
    private final List<Action> _postCommitActions = new ArrayList<Action>();

    private volatile TransactionLog.Transaction _transaction;
    private TransactionLog _transactionLog;

    public LocalTransaction(TransactionLog transactionLog)
    {
        _transactionLog = transactionLog;
    }

    public void addPostCommitAction(Action postCommitAction)
    {
        _postCommitActions.add(postCommitAction);
    }

    public void dequeue(AMQQueue queue, EnqueableMessage message, Action postCommitAction)
    {
        if(message.isPersistent() && queue.isDurable())
        {
            try
            {

                beginTranIfNecessary();
                _transaction.dequeueMessage(queue, message.getMessageNumber());

            }
            catch(AMQException e)
            {
                tidyUpOnError(e);
            }
        }
        _postCommitActions.add(postCommitAction);
    }

    public void dequeue(Collection<QueueEntry> queueEntries, Action postCommitAction)
    {
        try
        {

            for(QueueEntry entry : queueEntries)
            {
                ServerMessage message = entry.getMessage();
                AMQQueue queue = entry.getQueue();
                if(message.isPersistent() && queue.isDurable())
                {
                    beginTranIfNecessary();
                    _transaction.dequeueMessage(queue, message.getMessageNumber());
                }

            }
        }
        catch(AMQException e)
        {
            tidyUpOnError(e);
        }
        _postCommitActions.add(postCommitAction);

    }

    private void tidyUpOnError(Exception e)
    {
        try
        {
            for(Action action : _postCommitActions)
            {
                action.onRollback();
            }
        }
        finally
        {
            try
            {
                _transaction.abortTran();
            }
            catch (Exception e1)
            {
                // TODO could try to chain the information to the original error
            }
            _transaction = null;
            _postCommitActions.clear();
        }

        throw new RuntimeException(e);
    }

    private void beginTranIfNecessary()
    {
        if(_transaction == null)
        {
            try
            {
                _transaction = _transactionLog.newTransaction();
            }
            catch (Exception e)
            {
                tidyUpOnError(e);
            }
        }
    }

    public void enqueue(AMQQueue queue, EnqueableMessage message, Action postCommitAction)
    {
        if(message.isPersistent() && queue.isDurable())
        {
            beginTranIfNecessary();
            try
            {
                _transaction.enqueueMessage(queue, message.getMessageNumber());
            }
            catch (Exception e)
            {
                tidyUpOnError(e);
            }
        }
        _postCommitActions.add(postCommitAction);


    }

    public void enqueue(List<AMQQueue> queues, EnqueableMessage message, Action postCommitAction)
    {


        if(message.isPersistent())
        {
            if(_transaction == null)
            {
                for(AMQQueue queue : queues)
                {
                    if(queue.isDurable())
                    {
                        beginTranIfNecessary();
                        break;
                    }
                }


            }


            try
            {
                for(AMQQueue queue : queues)
                {
                    if(queue.isDurable())
                    {
                        _transaction.enqueueMessage(queue, message.getMessageNumber());
                    }
                }

            }
            catch (Exception e)
            {
                tidyUpOnError(e);
            }
        }
        _postCommitActions.add(postCommitAction);


    }

    public void commit()
    {
        try
        {
            if(_transaction != null)
            {

                _transaction.commitTran();
            }

            for(Action action : _postCommitActions)
            {
                action.postCommit();
            }
        }
        catch (Exception e)
        {
            for(Action action : _postCommitActions)
            {
                action.onRollback();
            }
            //TODO
            throw new RuntimeException(e);
        }
        finally
        {
            _transaction = null;
            _postCommitActions.clear();
        }

    }

    public void rollback()
    {

        try
        {

            if(_transaction != null)
            {

                _transaction.abortTran();
            }
        }
        catch (AMQException e)
        {
            //TODO
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        finally
        {
            try
            {
                for(Action action : _postCommitActions)
                {
                    action.onRollback();
                }
            }
            finally
            {
                _transaction = null;
                _postCommitActions.clear();
            }
        }
    }
}
