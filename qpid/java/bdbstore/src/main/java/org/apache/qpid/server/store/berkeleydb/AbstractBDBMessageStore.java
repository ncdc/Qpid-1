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
package org.apache.qpid.server.store.berkeleydb;

import com.sleepycat.bind.tuple.ByteBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.File;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.qpid.AMQStoreException;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.server.binding.Binding;
import org.apache.qpid.server.exchange.Exchange;
import org.apache.qpid.server.federation.Bridge;
import org.apache.qpid.server.federation.BrokerLink;
import org.apache.qpid.server.message.EnqueableMessage;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.store.*;
import org.apache.qpid.server.store.ConfigurationRecoveryHandler.BindingRecoveryHandler;
import org.apache.qpid.server.store.ConfigurationRecoveryHandler.BrokerLinkRecoveryHandler;
import org.apache.qpid.server.store.ConfigurationRecoveryHandler.ExchangeRecoveryHandler;
import org.apache.qpid.server.store.ConfigurationRecoveryHandler.QueueRecoveryHandler;
import org.apache.qpid.server.store.MessageStoreRecoveryHandler.StoredMessageRecoveryHandler;
import org.apache.qpid.server.store.TransactionLogRecoveryHandler.QueueEntryRecoveryHandler;
import org.apache.qpid.server.store.berkeleydb.entry.PreparedTransaction;
import org.apache.qpid.server.store.berkeleydb.entry.QueueEntryKey;
import org.apache.qpid.server.store.berkeleydb.entry.Xid;
import org.apache.qpid.server.store.berkeleydb.tuple.ConfiguredObjectBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.ContentBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.MessageMetaDataBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.PreparedTransactionBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.StringMapBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.UUIDTupleBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.XidBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.Upgrader;

public abstract class AbstractBDBMessageStore implements MessageStore
{
    private static final Logger LOGGER = Logger.getLogger(AbstractBDBMessageStore.class);

    private static final int LOCK_RETRY_ATTEMPTS = 5;

    public static final int VERSION = 6;

    private static final Map<String, String> ENVCONFIG_DEFAULTS = Collections.unmodifiableMap(new HashMap<String, String>()
    {{
        put(EnvironmentConfig.LOCK_N_LOCK_TABLES, "7");
    }});

    private Environment _environment;

    private String CONFIGURED_OBJECTS = "CONFIGURED_OBJECTS";
    private String MESSAGEMETADATADB_NAME = "MESSAGE_METADATA";
    private String MESSAGECONTENTDB_NAME = "MESSAGE_CONTENT";
    private String DELIVERYDB_NAME = "QUEUE_ENTRIES";
    private String BRIDGEDB_NAME = "BRIDGES";
    private String LINKDB_NAME = "LINKS";
    private String XIDDB_NAME = "XIDS";

    private Database _configuredObjectsDb;
    private Database _messageMetaDataDb;
    private Database _messageContentDb;
    private Database _deliveryDb;
    private Database _bridgeDb;
    private Database _linkDb;
    private Database _xidDb;

    /* =======
     * Schema:
     * =======
     *
     * Queue:
     * name(AMQShortString) - name(AMQShortString), owner(AMQShortString),
     *                        arguments(FieldTable encoded as binary), exclusive (boolean)
     *
     * Exchange:
     * name(AMQShortString) - name(AMQShortString), typeName(AMQShortString), autodelete (boolean)
     *
     * Binding:
     * exchangeName(AMQShortString), queueName(AMQShortString), routingKey(AMQShortString),
     *                                            arguments (FieldTable encoded as binary) - 0 (zero)
     *
     * QueueEntry:
     * queueName(AMQShortString), messageId (long) - 0 (zero)
     *
     * Message (MetaData):
     * messageId (long) - bodySize (integer), metaData (MessageMetaData encoded as binary)
     *
     * Message (Content):
     * messageId (long), byteOffset (integer) - dataLength(integer), data(binary)
     */

    private final AtomicLong _messageId = new AtomicLong(0);

    protected final StateManager _stateManager;

    private MessageStoreRecoveryHandler _messageRecoveryHandler;

    private TransactionLogRecoveryHandler _tlogRecoveryHandler;

    private ConfigurationRecoveryHandler _configRecoveryHandler;

    private long _totalStoreSize;
    private boolean _limitBusted;
    private long _persistentSizeLowThreshold;
    private long _persistentSizeHighThreshold;

    private final EventManager _eventManager = new EventManager();
    private String _storeLocation;

    private ConfiguredObjectHelper _configuredObjectHelper = new ConfiguredObjectHelper();

    private Map<String, String> _envConfigMap;

    public AbstractBDBMessageStore()
    {
        _stateManager = new StateManager(_eventManager);
    }

    @Override
    public void addEventListener(EventListener eventListener, Event... events)
    {
        _eventManager.addEventListener(eventListener, events);
    }

    public void configureConfigStore(String name,
                                     ConfigurationRecoveryHandler recoveryHandler,
                                     Configuration storeConfiguration) throws Exception
    {
        _stateManager.attainState(State.INITIALISING);

        _configRecoveryHandler = recoveryHandler;

        configure(name, storeConfiguration);
    }

    public void configureMessageStore(String name,
                                      MessageStoreRecoveryHandler messageRecoveryHandler,
                                      TransactionLogRecoveryHandler tlogRecoveryHandler,
                                      Configuration storeConfiguration) throws Exception
    {
        _messageRecoveryHandler = messageRecoveryHandler;
        _tlogRecoveryHandler = tlogRecoveryHandler;

        _stateManager.attainState(State.INITIALISED);
    }

    public synchronized void activate() throws Exception
    {
        _stateManager.attainState(State.ACTIVATING);

        recoverConfig(_configRecoveryHandler);
        recoverMessages(_messageRecoveryHandler);
        recoverQueueEntries(_tlogRecoveryHandler);

        _stateManager.attainState(State.ACTIVE);
    }

    public org.apache.qpid.server.store.Transaction newTransaction()
    {
        return new BDBTransaction();
    }

    /**
     * Called after instantiation in order to configure the message store.
     *
     * @param name The name of the virtual host using this store
     * @return whether a new store environment was created or not (to indicate whether recovery is necessary)
     *
     * @throws Exception If any error occurs that means the store is unable to configure itself.
     */
    public void configure(String name, Configuration storeConfig) throws Exception
    {
        final String storeLocation = storeConfig.getString(MessageStoreConstants.ENVIRONMENT_PATH_PROPERTY,
                System.getProperty("QPID_WORK") + File.separator + "bdbstore" + File.separator + name);

        _persistentSizeHighThreshold = storeConfig.getLong(MessageStoreConstants.OVERFULL_SIZE_PROPERTY, Long.MAX_VALUE);
        _persistentSizeLowThreshold = storeConfig.getLong(MessageStoreConstants.UNDERFULL_SIZE_PROPERTY, _persistentSizeHighThreshold);
        if(_persistentSizeLowThreshold > _persistentSizeHighThreshold || _persistentSizeLowThreshold < 0l)
        {
            _persistentSizeLowThreshold = _persistentSizeHighThreshold;
        }

        File environmentPath = new File(storeLocation);
        if (!environmentPath.exists())
        {
            if (!environmentPath.mkdirs())
            {
                throw new IllegalArgumentException("Environment path " + environmentPath + " could not be read or created. "
                                                   + "Ensure the path is correct and that the permissions are correct.");
            }
        }

        _storeLocation = storeLocation;

        _envConfigMap = getConfigMap(ENVCONFIG_DEFAULTS, storeConfig, "envConfig");

        LOGGER.info("Configuring BDB message store");

        setupStore(environmentPath, name);
    }

    protected Map<String,String> getConfigMap(Map<String, String> defaultConfig, Configuration config, String prefix) throws ConfigurationException
    {
        final List<Object> argumentNames = config.getList(prefix + ".name");
        final List<Object> argumentValues = config.getList(prefix + ".value");
        final int initialSize = argumentNames.size() + defaultConfig.size();

        final Map<String,String> attributes = new HashMap<String,String>(initialSize);
        attributes.putAll(defaultConfig);

        for (int i = 0; i < argumentNames.size(); i++)
        {
            final String argName = argumentNames.get(i).toString();
            final String argValue = argumentValues.get(i).toString();

            attributes.put(argName, argValue);
        }

        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public String getStoreLocation()
    {
        return _storeLocation;
    }

    /**
     * Move the store state from INITIAL to ACTIVE without actually recovering.
     *
     * This is required if you do not want to perform recovery of the store data
     *
     * @throws AMQStoreException if the store is not in the correct state
     */
    void startWithNoRecover() throws AMQStoreException
    {
        _stateManager.attainState(State.INITIALISING);
        _stateManager.attainState(State.INITIALISED);
        _stateManager.attainState(State.ACTIVATING);
        _stateManager.attainState(State.ACTIVE);
    }

    protected void setupStore(File storePath, String name) throws DatabaseException, AMQStoreException
    {
        _environment = createEnvironment(storePath);

        new Upgrader(_environment, name).upgradeIfNecessary();

        openDatabases();

        _totalStoreSize = getSizeOnDisk();
    }

    protected abstract Environment createEnvironment(File environmentPath) throws DatabaseException;

    public Environment getEnvironment()
    {
        return _environment;
    }

    private void openDatabases() throws DatabaseException
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        //This is required if we are wanting read only access.
        dbConfig.setReadOnly(false);

        _configuredObjectsDb = openDatabase(CONFIGURED_OBJECTS, dbConfig);
        _messageMetaDataDb = openDatabase(MESSAGEMETADATADB_NAME, dbConfig);
        _messageContentDb = openDatabase(MESSAGECONTENTDB_NAME, dbConfig);
        _deliveryDb = openDatabase(DELIVERYDB_NAME, dbConfig);
        _linkDb = openDatabase(LINKDB_NAME, dbConfig);
        _bridgeDb = openDatabase(BRIDGEDB_NAME, dbConfig);
        _xidDb = openDatabase(XIDDB_NAME, dbConfig);
    }

    private Database openDatabase(final String dbName, final DatabaseConfig dbConfig)
    {
        // if opening read-only and the database doesn't exist, then you can't create it
        return dbConfig.getReadOnly() && !_environment.getDatabaseNames().contains(dbName)
               ? null
               : _environment.openDatabase(null, dbName, dbConfig);
    }

    /**
     * Called to close and cleanup any resources used by the message store.
     *
     * @throws Exception If the close fails.
     */
    public void close() throws Exception
    {
        _stateManager.attainState(State.CLOSING);
        closeInternal();
        _stateManager.attainState(State.CLOSED);
    }

    protected void closeInternal() throws Exception
    {
        if (_messageMetaDataDb != null)
        {
            LOGGER.info("Closing message metadata database");
            _messageMetaDataDb.close();
        }

        if (_messageContentDb != null)
        {
            LOGGER.info("Closing message content database");
            _messageContentDb.close();
        }

         if (_configuredObjectsDb != null)
         {
             LOGGER.info("Closing configurable objects database");
             _configuredObjectsDb.close();
         }

        if (_deliveryDb != null)
        {
            LOGGER.info("Close delivery database");
            _deliveryDb.close();
        }

        if (_bridgeDb != null)
        {
            LOGGER.info("Close bridge database");
            _bridgeDb.close();
        }

        if (_linkDb != null)
        {
            LOGGER.info("Close link database");
            _linkDb.close();
        }


        if (_xidDb != null)
        {
            LOGGER.info("Close xid database");
            _xidDb.close();
        }

        closeEnvironment();

    }

    private void closeEnvironment() throws DatabaseException
    {
        if (_environment != null)
        {
            // Clean the log before closing. This makes sure it doesn't contain
            // redundant data. Closing without doing this means the cleaner may not
            // get a chance to finish.
            try
            {
                _environment.cleanLog();
            }
            finally
            {
                _environment.close();
            }
        }
    }


    private void recoverConfig(ConfigurationRecoveryHandler recoveryHandler) throws AMQStoreException
    {
        try
        {
            List<ConfiguredObjectRecord> configuredObjects = loadConfiguredObjects();
            ExchangeRecoveryHandler erh = recoveryHandler.begin(this);
            _configuredObjectHelper.recoverExchanges(erh, configuredObjects);

            QueueRecoveryHandler qrh = erh.completeExchangeRecovery();
            _configuredObjectHelper.recoverQueues(qrh, configuredObjects);

            BindingRecoveryHandler brh = qrh.completeQueueRecovery();
            _configuredObjectHelper.recoverBindings(brh, configuredObjects);

            BrokerLinkRecoveryHandler lrh = brh.completeBindingRecovery();
            recoverBrokerLinks(lrh);
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error recovering persistent state: " + e.getMessage(), e);
        }

    }

    private List<ConfiguredObjectRecord> loadConfiguredObjects() throws DatabaseException
    {
        Cursor cursor = null;
        List<ConfiguredObjectRecord> results = new ArrayList<ConfiguredObjectRecord>();
        try
        {
            cursor = _configuredObjectsDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            while (cursor.getNext(key, value, LockMode.RMW) == OperationStatus.SUCCESS)
            {
                ConfiguredObjectRecord configuredObject = ConfiguredObjectBinding.getInstance().entryToObject(value);
                UUID id = UUIDTupleBinding.getInstance().entryToObject(key);
                configuredObject.setId(id);
                results.add(configuredObject);
            }

        }
        finally
        {
            closeCursorSafely(cursor);
        }
        return results;
    }

    private void closeCursorSafely(Cursor cursor)
    {
        if (cursor != null)
        {
            cursor.close();
        }
    }

    private void recoverBrokerLinks(final ConfigurationRecoveryHandler.BrokerLinkRecoveryHandler lrh)
    {
        Cursor cursor = null;

        try
        {
            cursor = _linkDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();

            while (cursor.getNext(key, value, LockMode.RMW) == OperationStatus.SUCCESS)
            {
                UUID id = UUIDTupleBinding.getInstance().entryToObject(key);
                long createTime = LongBinding.entryToLong(value);
                Map<String,String> arguments = StringMapBinding.getInstance().entryToObject(value);

                ConfigurationRecoveryHandler.BridgeRecoveryHandler brh = lrh.brokerLink(id, createTime, arguments);

                recoverBridges(brh, id);
            }
        }
        finally
        {
            closeCursorSafely(cursor);
        }

    }

    private void recoverBridges(final ConfigurationRecoveryHandler.BridgeRecoveryHandler brh, final UUID linkId)
    {
        Cursor cursor = null;

        try
        {
            cursor = _bridgeDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();

            while (cursor.getNext(key, value, LockMode.RMW) == OperationStatus.SUCCESS)
            {
                UUID id = UUIDTupleBinding.getInstance().entryToObject(key);

                UUID parentId = UUIDTupleBinding.getInstance().entryToObject(value);
                if(parentId.equals(linkId))
                {

                    long createTime = LongBinding.entryToLong(value);
                    Map<String,String> arguments = StringMapBinding.getInstance().entryToObject(value);
                    brh.bridge(id,createTime,arguments);
                }
            }
            brh.completeBridgeRecoveryForLink();
        }
        finally
        {
            closeCursorSafely(cursor);
        }

    }


    private void recoverMessages(MessageStoreRecoveryHandler msrh) throws DatabaseException
    {
        StoredMessageRecoveryHandler mrh = msrh.begin();

        Cursor cursor = null;
        try
        {
            cursor = _messageMetaDataDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            MessageMetaDataBinding valueBinding = MessageMetaDataBinding.getInstance();

            long maxId = 0;

            while (cursor.getNext(key, value, LockMode.RMW) == OperationStatus.SUCCESS)
            {
                long messageId = LongBinding.entryToLong(key);
                StorableMessageMetaData metaData = valueBinding.entryToObject(value);

                StoredBDBMessage message = new StoredBDBMessage(messageId, metaData, false);

                mrh.message(message);

                maxId = Math.max(maxId, messageId);
            }

            _messageId.set(maxId);
        }
        catch (DatabaseException e)
        {
            LOGGER.error("Database Error: " + e.getMessage(), e);
            throw e;
        }
        finally
        {
            closeCursorSafely(cursor);
        }
    }

    private void recoverQueueEntries(TransactionLogRecoveryHandler recoveryHandler)
    throws DatabaseException
    {
        QueueEntryRecoveryHandler qerh = recoveryHandler.begin(this);

        ArrayList<QueueEntryKey> entries = new ArrayList<QueueEntryKey>();

        Cursor cursor = null;
        try
        {
            cursor = _deliveryDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            QueueEntryBinding keyBinding = QueueEntryBinding.getInstance();

            DatabaseEntry value = new DatabaseEntry();
            while (cursor.getNext(key, value, LockMode.RMW) == OperationStatus.SUCCESS)
            {
                QueueEntryKey qek = keyBinding.entryToObject(key);

                entries.add(qek);
            }

            try
            {
                cursor.close();
            }
            finally
            {
                cursor = null;
            }

            for(QueueEntryKey entry : entries)
            {
                UUID queueId = entry.getQueueId();
                long messageId = entry.getMessageId();
                qerh.queueEntry(queueId, messageId);
            }
        }
        catch (DatabaseException e)
        {
            LOGGER.error("Database Error: " + e.getMessage(), e);
            throw e;
        }
        finally
        {
            closeCursorSafely(cursor);
        }

        TransactionLogRecoveryHandler.DtxRecordRecoveryHandler dtxrh = qerh.completeQueueEntryRecovery();

        cursor = null;
        try
        {
            cursor = _xidDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            XidBinding keyBinding = XidBinding.getInstance();
            PreparedTransactionBinding valueBinding = new PreparedTransactionBinding();
            DatabaseEntry value = new DatabaseEntry();

            while (cursor.getNext(key, value, LockMode.RMW) == OperationStatus.SUCCESS)
            {
                Xid xid = keyBinding.entryToObject(key);
                PreparedTransaction preparedTransaction = valueBinding.entryToObject(value);
                dtxrh.dtxRecord(xid.getFormat(),xid.getGlobalId(),xid.getBranchId(),
                                preparedTransaction.getEnqueues(),preparedTransaction.getDequeues());
            }

        }
        catch (DatabaseException e)
        {
            LOGGER.error("Database Error: " + e.getMessage(), e);
            throw e;
        }
        finally
        {
            closeCursorSafely(cursor);
        }


        dtxrh.completeDtxRecordRecovery();
    }

    public void removeMessage(long messageId, boolean sync) throws AMQStoreException
    {

        boolean complete = false;
        com.sleepycat.je.Transaction tx = null;

        Random rand = null;
        int attempts = 0;
        try
        {
            do
            {
                tx = null;
                try
                {
                    tx = _environment.beginTransaction(null, null);

                    //remove the message meta data from the store
                    DatabaseEntry key = new DatabaseEntry();
                    LongBinding.longToEntry(messageId, key);

                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Removing message id " + messageId);
                    }


                    OperationStatus status = _messageMetaDataDb.delete(tx, key);
                    if (status == OperationStatus.NOTFOUND)
                    {
                        LOGGER.info("Message not found (attempt to remove failed - probably application initiated rollback) " +
                        messageId);
                    }

                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Deleted metadata for message " + messageId);
                    }

                    //now remove the content data from the store if there is any.
                    DatabaseEntry contentKeyEntry = new DatabaseEntry();
                    LongBinding.longToEntry(messageId, contentKeyEntry);
                    _messageContentDb.delete(tx, contentKeyEntry);

                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Deleted content for message " + messageId);
                    }

                    commit(tx, sync);
                    complete = true;
                    tx = null;
                }
                catch (LockConflictException e)
                {
                    try
                    {
                        if(tx != null)
                        {
                            tx.abort();
                        }
                    }
                    catch(DatabaseException e2)
                    {
                        LOGGER.warn("Unable to abort transaction after LockConflictExcption", e2);
                        // rethrow the original log conflict exception, the secondary exception should already have
                        // been logged.
                        throw e;
                    }


                    LOGGER.warn("Lock timeout exception. Retrying (attempt "
                              + (attempts+1) + " of "+ LOCK_RETRY_ATTEMPTS +") " + e);

                    if(++attempts < LOCK_RETRY_ATTEMPTS)
                    {
                        if(rand == null)
                        {
                            rand = new Random();
                        }

                        try
                        {
                            Thread.sleep(500l + (long)(500l * rand.nextDouble()));
                        }
                        catch (InterruptedException e1)
                        {

                        }
                    }
                    else
                    {
                        // rethrow the lock conflict exception since we could not solve by retrying
                        throw e;
                    }
                }
            }
            while(!complete);
        }
        catch (DatabaseException e)
        {
            LOGGER.error("Unexpected BDB exception", e);

            if (tx != null)
            {
                try
                {
                    tx.abort();
                    tx = null;
                }
                catch (DatabaseException e1)
                {
                    throw new AMQStoreException("Error aborting transaction " + e1, e1);
                }
            }

            throw new AMQStoreException("Error removing message with id " + messageId + " from database: " + e.getMessage(), e);
        }
        finally
        {
            if (tx != null)
            {
                try
                {
                    tx.abort();
                    tx = null;
                }
                catch (DatabaseException e1)
                {
                    throw new AMQStoreException("Error aborting transaction " + e1, e1);
                }
            }
        }
    }

    /**
     * @see DurableConfigurationStore#createExchange(Exchange)
     */
    public void createExchange(Exchange exchange) throws AMQStoreException
    {
        if (_stateManager.isInState(State.ACTIVE))
        {
            ConfiguredObjectRecord configuredObject = _configuredObjectHelper.createExchangeConfiguredObject(exchange);
            storeConfiguredObjectEntry(configuredObject);
        }
    }

    /**
     * @see DurableConfigurationStore#removeExchange(Exchange)
     */
    public void removeExchange(Exchange exchange) throws AMQStoreException
    {
        UUID id = exchange.getId();
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("public void removeExchange(String name = " + exchange.getName() + ", uuid = " + id + "): called");
        }
        OperationStatus status = removeConfiguredObject(id);
        if (status == OperationStatus.NOTFOUND)
        {
            throw new AMQStoreException("Exchange " + exchange.getName() + " with id " + id + " not found");
        }
    }


    /**
     * @see DurableConfigurationStore#bindQueue(Binding)
     */
    public void bindQueue(Binding binding) throws AMQStoreException
    {
        if (_stateManager.isInState(State.ACTIVE))
        {
            ConfiguredObjectRecord configuredObject = _configuredObjectHelper.createBindingConfiguredObject(binding);
            storeConfiguredObjectEntry(configuredObject);
        }
    }

    /**
     * @see DurableConfigurationStore#unbindQueue(Binding)
     */
    public void unbindQueue(Binding binding)
            throws AMQStoreException
    {
        UUID id = binding.getId();
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("public void unbindQueue(Binding binding = " + binding + ", uuid = " + id + "): called");
        }

        OperationStatus status = removeConfiguredObject(id);
        if (status == OperationStatus.NOTFOUND)
        {
            throw new AMQStoreException("Binding " + binding + " not found");
        }
    }

    /**
     * @see DurableConfigurationStore#createQueue(AMQQueue)
     */
    public void createQueue(AMQQueue queue) throws AMQStoreException
    {
        createQueue(queue, null);
    }

    /**
     * @see DurableConfigurationStore#createQueue(AMQQueue, FieldTable)
     */
    public void createQueue(AMQQueue queue, FieldTable arguments) throws AMQStoreException
    {
        if (_stateManager.isInState(State.ACTIVE))
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("public void createQueue(AMQQueue queue(" + queue.getName() + "), queue id" + queue.getId()
                        + ", arguments=" + arguments + "): called");
            }
            ConfiguredObjectRecord configuredObject = _configuredObjectHelper.createQueueConfiguredObject(queue, arguments);
            storeConfiguredObjectEntry(configuredObject);
        }
    }

    /**
     * Updates the specified queue in the persistent store, IF it is already present. If the queue
     * is not present in the store, it will not be added.
     *
     * @param queue The queue to update the entry for.
     * @throws AMQStoreException If the operation fails for any reason.
     */
    public void updateQueue(final AMQQueue queue) throws AMQStoreException
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Updating queue: " + queue.getName());
        }

        try
        {
            DatabaseEntry key = new DatabaseEntry();
            UUIDTupleBinding keyBinding = UUIDTupleBinding.getInstance();
            keyBinding.objectToEntry(queue.getId(), key);

            DatabaseEntry value = new DatabaseEntry();
            DatabaseEntry newValue = new DatabaseEntry();
            ConfiguredObjectBinding configuredObjectBinding = ConfiguredObjectBinding.getInstance();

            OperationStatus status = _configuredObjectsDb.get(null, key, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
            {
                ConfiguredObjectRecord queueRecord = configuredObjectBinding.entryToObject(value);
                ConfiguredObjectRecord newQueueRecord = _configuredObjectHelper.updateQueueConfiguredObject(queue, queueRecord);

                // write the updated entry to the store
                configuredObjectBinding.objectToEntry(newQueueRecord, newValue);
                status = _configuredObjectsDb.put(null, key, newValue);
                if (status != OperationStatus.SUCCESS)
                {
                    throw new AMQStoreException("Error updating queue details within the store: " + status);
                }
            }
            else if (status != OperationStatus.NOTFOUND)
            {
                throw new AMQStoreException("Error finding queue details within the store: " + status);
            }
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error updating queue details within the store: " + e,e);
        }
    }

    /**
     * Removes the specified queue from the persistent store.
     *
     * @param queue The queue to remove.
     *
     * @throws AMQStoreException If the operation fails for any reason.
     */
    public void removeQueue(final AMQQueue queue) throws AMQStoreException
    {
        UUID id = queue.getId();
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("public void removeQueue(AMQShortString name = " + queue.getName() + ", uuid = " + id + "): called");
        }

        OperationStatus status = removeConfiguredObject(id);
        if (status == OperationStatus.NOTFOUND)
        {
            throw new AMQStoreException("Queue " + queue.getName() + " with id " + id + " not found");
        }
    }

    public void createBrokerLink(final BrokerLink link) throws AMQStoreException
    {
        if (_stateManager.isInState(State.ACTIVE))
        {
            DatabaseEntry key = new DatabaseEntry();
            UUIDTupleBinding.getInstance().objectToEntry(link.getId(), key);

            DatabaseEntry value = new DatabaseEntry();
            LongBinding.longToEntry(link.getCreateTime(), value);
            StringMapBinding.getInstance().objectToEntry(link.getArguments(), value);

            try
            {
                _linkDb.put(null, key, value);
            }
            catch (DatabaseException e)
            {
                throw new AMQStoreException("Error writing Link  " + link
                                            + " to database: " + e.getMessage(), e);
            }
        }
    }

    public void deleteBrokerLink(final BrokerLink link) throws AMQStoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        UUIDTupleBinding.getInstance().objectToEntry(link.getId(), key);
        try
        {
            OperationStatus status = _linkDb.delete(null, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new AMQStoreException("Link " + link + " not found");
            }
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error deleting the Link " + link + " from database: " + e.getMessage(), e);
        }
    }

    public void createBridge(final Bridge bridge) throws AMQStoreException
    {
        if (_stateManager.isInState(State.ACTIVE))
        {
            DatabaseEntry key = new DatabaseEntry();
            UUIDTupleBinding.getInstance().objectToEntry(bridge.getId(), key);

            DatabaseEntry value = new DatabaseEntry();
            UUIDTupleBinding.getInstance().objectToEntry(bridge.getLink().getId(),value);
            LongBinding.longToEntry(bridge.getCreateTime(),value);
            StringMapBinding.getInstance().objectToEntry(bridge.getArguments(), value);

            try
            {
                _bridgeDb.put(null, key, value);
            }
            catch (DatabaseException e)
            {
                throw new AMQStoreException("Error writing Bridge  " + bridge
                                            + " to database: " + e.getMessage(), e);
            }

        }
    }

    public void deleteBridge(final Bridge bridge) throws AMQStoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        UUIDTupleBinding.getInstance().objectToEntry(bridge.getId(), key);
        try
        {
            OperationStatus status = _bridgeDb.delete(null, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new AMQStoreException("Bridge " + bridge + " not found");
            }
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error deleting the Bridge " + bridge + " from database: " + e.getMessage(), e);
        }
    }

    /**
     * Places a message onto a specified queue, in a given transaction.
     *
     * @param tx   The transaction for the operation.
     * @param queue     The the queue to place the message on.
     * @param messageId The message to enqueue.
     *
     * @throws AMQStoreException If the operation fails for any reason.
     */
    public void enqueueMessage(final com.sleepycat.je.Transaction tx, final TransactionLogResource queue,
                               long messageId) throws AMQStoreException
    {

        DatabaseEntry key = new DatabaseEntry();
        QueueEntryBinding keyBinding = QueueEntryBinding.getInstance();
        QueueEntryKey dd = new QueueEntryKey(queue.getId(), messageId);
        keyBinding.objectToEntry(dd, key);
        DatabaseEntry value = new DatabaseEntry();
        ByteBinding.byteToEntry((byte) 0, value);

        try
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Enqueuing message " + messageId + " on queue "
                        + (queue instanceof AMQQueue ? ((AMQQueue) queue).getName() + " with id " : "") + queue.getId()
                        + " [Transaction" + tx + "]");
            }
            _deliveryDb.put(tx, key, value);
        }
        catch (DatabaseException e)
        {
            LOGGER.error("Failed to enqueue: " + e.getMessage(), e);
            throw new AMQStoreException("Error writing enqueued message with id " + messageId + " for queue "
                    + (queue instanceof AMQQueue ? ((AMQQueue) queue).getName() + " with id " : "") + queue.getId()
                    + " to database", e);
        }
    }

    /**
     * Extracts a message from a specified queue, in a given transaction.
     *
     * @param tx   The transaction for the operation.
     * @param queue     The queue to take the message from.
     * @param messageId The message to dequeue.
     *
     * @throws AMQStoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    public void dequeueMessage(final com.sleepycat.je.Transaction tx, final TransactionLogResource queue,
                               long messageId) throws AMQStoreException
    {

        DatabaseEntry key = new DatabaseEntry();
        QueueEntryBinding keyBinding = QueueEntryBinding.getInstance();
        QueueEntryKey queueEntryKey = new QueueEntryKey(queue.getId(), messageId);
        UUID id = queue.getId();
        keyBinding.objectToEntry(queueEntryKey, key);
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Dequeue message id " + messageId + " from queue "
                    + (queue instanceof AMQQueue ? ((AMQQueue) queue).getName() + " with id " : "") + id);
        }

        try
        {

            OperationStatus status = _deliveryDb.delete(tx, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new AMQStoreException("Unable to find message with id " + messageId + " on queue "
                        + (queue instanceof AMQQueue ? ((AMQQueue) queue).getName() + " with id " : "") + id);
            }
            else if (status != OperationStatus.SUCCESS)
            {
                throw new AMQStoreException("Unable to remove message with id " + messageId + " on queue"
                        + (queue instanceof AMQQueue ? ((AMQQueue) queue).getName() + " with id " : "") + id);
            }

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Removed message " + messageId + " on queue "
                        + (queue instanceof AMQQueue ? ((AMQQueue) queue).getName() + " with id " : "") + id
                        + " from delivery db");

            }
        }
        catch (DatabaseException e)
        {

            LOGGER.error("Failed to dequeue message " + messageId + ": " + e.getMessage(), e);
            LOGGER.error(tx);

            throw new AMQStoreException("Error accessing database while dequeuing message: " + e.getMessage(), e);
        }
    }


    private void recordXid(com.sleepycat.je.Transaction txn,
                           long format,
                           byte[] globalId,
                           byte[] branchId,
                           org.apache.qpid.server.store.Transaction.Record[] enqueues,
                           org.apache.qpid.server.store.Transaction.Record[] dequeues) throws AMQStoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        Xid xid = new Xid(format, globalId, branchId);
        XidBinding keyBinding = XidBinding.getInstance();
        keyBinding.objectToEntry(xid,key);

        DatabaseEntry value = new DatabaseEntry();
        PreparedTransaction preparedTransaction = new PreparedTransaction(enqueues, dequeues);
        PreparedTransactionBinding valueBinding = new PreparedTransactionBinding();
        valueBinding.objectToEntry(preparedTransaction, value);

        try
        {
            _xidDb.put(txn, key, value);
        }
        catch (DatabaseException e)
        {
            LOGGER.error("Failed to write xid: " + e.getMessage(), e);
            throw new AMQStoreException("Error writing xid to database", e);
        }
    }

    private void removeXid(com.sleepycat.je.Transaction txn, long format, byte[] globalId, byte[] branchId)
            throws AMQStoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        Xid xid = new Xid(format, globalId, branchId);
        XidBinding keyBinding = XidBinding.getInstance();

        keyBinding.objectToEntry(xid, key);


        try
        {

            OperationStatus status = _xidDb.delete(txn, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new AMQStoreException("Unable to find xid");
            }
            else if (status != OperationStatus.SUCCESS)
            {
                throw new AMQStoreException("Unable to remove xid");
            }

        }
        catch (DatabaseException e)
        {

            LOGGER.error("Failed to remove xid ", e);
            LOGGER.error(txn);

            throw new AMQStoreException("Error accessing database while removing xid: " + e.getMessage(), e);
        }
    }

    /**
     * Commits all operations performed within a given transaction.
     *
     * @param tx The transaction to commit all operations for.
     *
     * @throws AMQStoreException If the operation fails for any reason.
     */
    private StoreFuture commitTranImpl(final com.sleepycat.je.Transaction tx, boolean syncCommit) throws AMQStoreException
    {
        if (tx == null)
        {
            throw new AMQStoreException("Fatal internal error: transactional is null at commitTran");
        }

        StoreFuture result;
        try
        {
            result = commit(tx, syncCommit);

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("commitTranImpl completed for [Transaction:" + tx + "]");
            }
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error commit tx: " + e.getMessage(), e);
        }

        return result;
    }

    /**
     * Abandons all operations performed within a given transaction.
     *
     * @param tx The transaction to abandon.
     *
     * @throws AMQStoreException If the operation fails for any reason.
     */
    public void abortTran(final com.sleepycat.je.Transaction tx) throws AMQStoreException
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("abortTran called for [Transaction:" + tx + "]");
        }

        try
        {
            tx.abort();
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error aborting transaction: " + e.getMessage(), e);
        }
    }

    /**
     * Primarily for testing purposes.
     *
     * @param queueId
     *
     * @return a list of message ids for messages enqueued for a particular queue
     */
    List<Long> getEnqueuedMessages(UUID queueId) throws AMQStoreException
    {
        Cursor cursor = null;
        try
        {
            cursor = _deliveryDb.openCursor(null, null);

            DatabaseEntry key = new DatabaseEntry();

            QueueEntryKey dd = new QueueEntryKey(queueId, 0);

            QueueEntryBinding keyBinding = QueueEntryBinding.getInstance();
            keyBinding.objectToEntry(dd, key);

            DatabaseEntry value = new DatabaseEntry();

            LinkedList<Long> messageIds = new LinkedList<Long>();

            OperationStatus status = cursor.getSearchKeyRange(key, value, LockMode.DEFAULT);
            dd = keyBinding.entryToObject(key);

            while ((status == OperationStatus.SUCCESS) && dd.getQueueId().equals(queueId))
            {

                messageIds.add(dd.getMessageId());
                status = cursor.getNext(key, value, LockMode.DEFAULT);
                if (status == OperationStatus.SUCCESS)
                {
                    dd = keyBinding.entryToObject(key);
                }
            }

            return messageIds;
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Database error: " + e.getMessage(), e);
        }
        finally
        {
            if (cursor != null)
            {
                try
                {
                    cursor.close();
                }
                catch (DatabaseException e)
                {
                    throw new AMQStoreException("Error closing cursor: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Return a valid, currently unused message id.
     *
     * @return A fresh message id.
     */
    public long getNewMessageId()
    {
        return _messageId.incrementAndGet();
    }

    /**
     * Stores a chunk of message data.
     *
     * @param tx         The transaction for the operation.
     * @param messageId       The message to store the data for.
     * @param offset          The offset of the data chunk in the message.
     * @param contentBody     The content of the data chunk.
     *
     * @throws AMQStoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    protected void addContent(final com.sleepycat.je.Transaction tx, long messageId, int offset,
                                      ByteBuffer contentBody) throws AMQStoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(messageId, key);
        DatabaseEntry value = new DatabaseEntry();
        ContentBinding messageBinding = ContentBinding.getInstance();
        messageBinding.objectToEntry(contentBody.array(), value);
        try
        {
            OperationStatus status = _messageContentDb.put(tx, key, value);
            if (status != OperationStatus.SUCCESS)
            {
                throw new AMQStoreException("Error adding content for message id " + messageId + ": " + status);
            }

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Storing content for message " + messageId + "[Transaction" + tx + "]");

            }
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error writing AMQMessage with id " + messageId + " to database: " + e.getMessage(), e);
        }
    }

    /**
     * Stores message meta-data.
     *
     * @param tx         The transaction for the operation.
     * @param messageId       The message to store the data for.
     * @param messageMetaData The message meta data to store.
     *
     * @throws AMQStoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    private void storeMetaData(final com.sleepycat.je.Transaction tx, long messageId,
                               StorableMessageMetaData messageMetaData)
            throws AMQStoreException
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("public void storeMetaData(Txn tx = " + tx + ", Long messageId = "
                       + messageId + ", MessageMetaData messageMetaData = " + messageMetaData + "): called");
        }

        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(messageId, key);
        DatabaseEntry value = new DatabaseEntry();

        MessageMetaDataBinding messageBinding = MessageMetaDataBinding.getInstance();
        messageBinding.objectToEntry(messageMetaData, value);
        try
        {
            _messageMetaDataDb.put(tx, key, value);
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Storing message metadata for message id " + messageId + "[Transaction" + tx + "]");
            }
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error writing message metadata with id " + messageId + " to database: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves message meta-data.
     *
     * @param messageId The message to get the meta-data for.
     *
     * @return The message meta data.
     *
     * @throws AMQStoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    public StorableMessageMetaData getMessageMetaData(long messageId) throws AMQStoreException
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("public MessageMetaData getMessageMetaData(Long messageId = "
                       + messageId + "): called");
        }

        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(messageId, key);
        DatabaseEntry value = new DatabaseEntry();
        MessageMetaDataBinding messageBinding = MessageMetaDataBinding.getInstance();

        try
        {
            OperationStatus status = _messageMetaDataDb.get(null, key, value, LockMode.READ_UNCOMMITTED);
            if (status != OperationStatus.SUCCESS)
            {
                throw new AMQStoreException("Metadata not found for message with id " + messageId);
            }

            StorableMessageMetaData mdd = messageBinding.entryToObject(value);

            return mdd;
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error reading message metadata for message with id " + messageId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Fills the provided ByteBuffer with as much content for the specified message as possible, starting
     * from the specified offset in the message.
     *
     * @param messageId The message to get the data for.
     * @param offset    The offset of the data within the message.
     * @param dst       The destination of the content read back
     *
     * @return The number of bytes inserted into the destination
     *
     * @throws AMQStoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    public int getContent(long messageId, int offset, ByteBuffer dst) throws AMQStoreException
    {
        DatabaseEntry contentKeyEntry = new DatabaseEntry();
        LongBinding.longToEntry(messageId, contentKeyEntry);
        DatabaseEntry value = new DatabaseEntry();
        ContentBinding contentTupleBinding = ContentBinding.getInstance();


        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Message Id: " + messageId + " Getting content body from offset: " + offset);
        }

        try
        {

            int written = 0;
            OperationStatus status = _messageContentDb.get(null, contentKeyEntry, value, LockMode.READ_UNCOMMITTED);
            if (status == OperationStatus.SUCCESS)
            {
                byte[] dataAsBytes = contentTupleBinding.entryToObject(value);
                int size = dataAsBytes.length;
                if (offset > size)
                {
                    throw new RuntimeException("Offset " + offset + " is greater than message size " + size
                            + " for message id " + messageId + "!");

                }

                written = size - offset;
                if(written > dst.remaining())
                {
                    written = dst.remaining();
                }

                dst.put(dataAsBytes, offset, written);
            }
            return written;
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error getting AMQMessage with id " + messageId + " to database: " + e.getMessage(), e);
        }
    }

    public boolean isPersistent()
    {
        return true;
    }

    @SuppressWarnings("unchecked")
    public <T extends StorableMessageMetaData> StoredMessage<T> addMessage(T metaData)
    {
        if(metaData.isPersistent())
        {
            return (StoredMessage<T>) new StoredBDBMessage(getNewMessageId(), metaData);
        }
        else
        {
            return new StoredMemoryMessage(getNewMessageId(), metaData);
        }
    }

    //Package getters for the various databases used by the Store

    Database getMetaDataDb()
    {
        return _messageMetaDataDb;
    }

    Database getContentDb()
    {
        return _messageContentDb;
    }

    Database getDeliveryDb()
    {
        return _deliveryDb;
    }

    /**
     * Makes the specified configured object persistent.
     *
     * @param configuredObject     Details of the configured object to store.
     * @throws AMQStoreException If the operation fails for any reason.
     */
    private void storeConfiguredObjectEntry(ConfiguredObjectRecord configuredObject) throws AMQStoreException
    {
        if (_stateManager.isInState(State.ACTIVE))
        {
            DatabaseEntry key = new DatabaseEntry();
            UUIDTupleBinding keyBinding = UUIDTupleBinding.getInstance();
            keyBinding.objectToEntry(configuredObject.getId(), key);

            DatabaseEntry value = new DatabaseEntry();
            ConfiguredObjectBinding queueBinding = ConfiguredObjectBinding.getInstance();

            queueBinding.objectToEntry(configuredObject, value);
            try
            {
                OperationStatus status = _configuredObjectsDb.put(null, key, value);
                if (status != OperationStatus.SUCCESS)
                {
                    throw new AMQStoreException("Error writing configured object " + configuredObject + " to database: "
                            + status);
                }
            }
            catch (DatabaseException e)
            {
                throw new AMQStoreException("Error writing configured object " + configuredObject
                        + " to database: " + e.getMessage(), e);
            }
        }
    }

    private OperationStatus removeConfiguredObject(UUID id) throws AMQStoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        UUIDTupleBinding uuidBinding = UUIDTupleBinding.getInstance();
        uuidBinding.objectToEntry(id, key);
        try
        {
            return _configuredObjectsDb.delete(null, key);
        }
        catch (DatabaseException e)
        {
            throw new AMQStoreException("Error deleting of configured object with id " + id + " from database", e);
        }
    }

    protected abstract StoreFuture commit(com.sleepycat.je.Transaction tx, boolean syncCommit) throws DatabaseException;


    private class StoredBDBMessage implements StoredMessage<StorableMessageMetaData>
    {

        private final long _messageId;
        private volatile SoftReference<StorableMessageMetaData> _metaDataRef;

        private StorableMessageMetaData _metaData;
        private volatile SoftReference<byte[]> _dataRef;
        private byte[] _data;

        StoredBDBMessage(long messageId, StorableMessageMetaData metaData)
        {
            this(messageId, metaData, true);
        }


        StoredBDBMessage(long messageId,
                           StorableMessageMetaData metaData, boolean persist)
        {
            try
            {
                _messageId = messageId;
                _metaData = metaData;

                _metaDataRef = new SoftReference<StorableMessageMetaData>(metaData);

            }
            catch (DatabaseException e)
            {
                throw new RuntimeException(e);
            }

        }

        public StorableMessageMetaData getMetaData()
        {
            StorableMessageMetaData metaData = _metaDataRef.get();
            if(metaData == null)
            {
                try
                {
                    metaData = AbstractBDBMessageStore.this.getMessageMetaData(_messageId);
                }
                catch (AMQStoreException e)
                {
                    throw new RuntimeException(e);
                }
                _metaDataRef = new SoftReference<StorableMessageMetaData>(metaData);
            }

            return metaData;
        }

        public long getMessageNumber()
        {
            return _messageId;
        }

        public void addContent(int offsetInMessage, java.nio.ByteBuffer src)
        {
            src = src.slice();

            if(_data == null)
            {
                _data = new byte[src.remaining()];
                _dataRef = new SoftReference<byte[]>(_data);
                src.duplicate().get(_data);
            }
            else
            {
                byte[] oldData = _data;
                _data = new byte[oldData.length + src.remaining()];
                _dataRef = new SoftReference<byte[]>(_data);

                System.arraycopy(oldData,0,_data,0,oldData.length);
                src.duplicate().get(_data, oldData.length, src.remaining());
            }

        }

        public int getContent(int offsetInMessage, java.nio.ByteBuffer dst)
        {
            byte[] data = _dataRef == null ? null : _dataRef.get();
            if(data != null)
            {
                int length = Math.min(dst.remaining(), data.length - offsetInMessage);
                dst.put(data, offsetInMessage, length);
                return length;
            }
            else
            {
                try
                {
                    return AbstractBDBMessageStore.this.getContent(_messageId, offsetInMessage, dst);
                }
                catch (AMQStoreException e)
                {
                    // TODO maybe should throw a checked exception, or at least log before throwing
                    throw new RuntimeException(e);
                }
            }
        }

        public ByteBuffer getContent(int offsetInMessage, int size)
        {
            byte[] data = _dataRef == null ? null : _dataRef.get();
            if(data != null)
            {
                return ByteBuffer.wrap(data,offsetInMessage,size);
            }
            else
            {
                ByteBuffer buf = ByteBuffer.allocate(size);
                getContent(offsetInMessage, buf);
                buf.position(0);
                return  buf;
            }
        }

        synchronized void store(com.sleepycat.je.Transaction txn)
        {

            if(unstored())
            {
                try
                {
                    _dataRef = new SoftReference<byte[]>(_data);
                    AbstractBDBMessageStore.this.storeMetaData(txn, _messageId, _metaData);
                    AbstractBDBMessageStore.this.addContent(txn, _messageId, 0,
                                                    _data == null ? ByteBuffer.allocate(0) : ByteBuffer.wrap(_data));
                }
                catch(DatabaseException e)
                {
                    throw new RuntimeException(e);
                }
                catch (AMQStoreException e)
                {
                    throw new RuntimeException(e);
                }
                catch (RuntimeException e)
                {
                    LOGGER.error("RuntimeException during store", e);
                    throw e;
                }
                finally
                {
                    _metaData = null;
                    _data = null;
                }
            }
        }

        private boolean unstored()
        {
            return _metaData != null;
        }

        public synchronized StoreFuture flushToStore()
        {
            if(unstored())
            {
                com.sleepycat.je.Transaction txn = _environment.beginTransaction(null, null);
                store(txn);
                AbstractBDBMessageStore.this.commit(txn,true);
                storedSizeChange(getMetaData().getContentSize());
            }
            return StoreFuture.IMMEDIATE_FUTURE;
        }

        public void remove()
        {
            try
            {
                int delta = getMetaData().getContentSize();
                AbstractBDBMessageStore.this.removeMessage(_messageId, false);
                storedSizeChange(-delta);

            }
            catch (AMQStoreException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private class BDBTransaction implements org.apache.qpid.server.store.Transaction
    {
        private com.sleepycat.je.Transaction _txn;
        private int _storeSizeIncrease;

        private BDBTransaction()
        {
            try
            {
                _txn = _environment.beginTransaction(null, null);
            }
            catch (DatabaseException e)
            {
                LOGGER.error("Exception during transaction begin, closing store environment.", e);
                closeEnvironmentSafely();

                throw new RuntimeException("Exception during transaction begin, store environment closed.", e);
            }
        }

        public void enqueueMessage(TransactionLogResource queue, EnqueableMessage message) throws AMQStoreException
        {
            if(message.getStoredMessage() instanceof StoredBDBMessage)
            {
                final StoredBDBMessage storedMessage = (StoredBDBMessage) message.getStoredMessage();
                storedMessage.store(_txn);
                _storeSizeIncrease += storedMessage.getMetaData().getContentSize();
            }

            AbstractBDBMessageStore.this.enqueueMessage(_txn, queue, message.getMessageNumber());
        }

        public void dequeueMessage(TransactionLogResource queue, EnqueableMessage message) throws AMQStoreException
        {
            AbstractBDBMessageStore.this.dequeueMessage(_txn, queue, message.getMessageNumber());
        }

        public void commitTran() throws AMQStoreException
        {
            AbstractBDBMessageStore.this.commitTranImpl(_txn, true);
            AbstractBDBMessageStore.this.storedSizeChange(_storeSizeIncrease);
        }

        public StoreFuture commitTranAsync() throws AMQStoreException
        {
            AbstractBDBMessageStore.this.storedSizeChange(_storeSizeIncrease);
            return AbstractBDBMessageStore.this.commitTranImpl(_txn, false);
        }

        public void abortTran() throws AMQStoreException
        {
            AbstractBDBMessageStore.this.abortTran(_txn);
        }

        public void removeXid(long format, byte[] globalId, byte[] branchId) throws AMQStoreException
        {
            AbstractBDBMessageStore.this.removeXid(_txn, format, globalId, branchId);
        }

        public void recordXid(long format, byte[] globalId, byte[] branchId, Record[] enqueues,
                              Record[] dequeues) throws AMQStoreException
        {
            AbstractBDBMessageStore.this.recordXid(_txn, format, globalId, branchId, enqueues, dequeues);
        }
    }

    private void storedSizeChange(final int delta)
    {
        if(getPersistentSizeHighThreshold() > 0)
        {
            synchronized (this)
            {
                // the delta supplied is an approximation of a store size change. we don;t want to check the statistic every
                // time, so we do so only when there's been enough change that it is worth looking again. We do this by
                // assuming the total size will change by less than twice the amount of the message data change.
                long newSize = _totalStoreSize += 2*delta;

                if(!_limitBusted &&  newSize > getPersistentSizeHighThreshold())
                {
                    _totalStoreSize = getSizeOnDisk();

                    if(_totalStoreSize > getPersistentSizeHighThreshold())
                    {
                        _limitBusted = true;
                        _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
                    }
                }
                else if(_limitBusted && newSize < getPersistentSizeLowThreshold())
                {
                    long oldSize = _totalStoreSize;
                    _totalStoreSize = getSizeOnDisk();

                    if(oldSize <= _totalStoreSize)
                    {

                        reduceSizeOnDisk();

                        _totalStoreSize = getSizeOnDisk();

                    }

                    if(_totalStoreSize < getPersistentSizeLowThreshold())
                    {
                        _limitBusted = false;
                        _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);
                    }


                }
            }
        }
    }

    private void reduceSizeOnDisk()
    {
        _environment.getConfig().setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        boolean cleaned = false;
        while (_environment.cleanLog() > 0)
        {
            cleaned = true;
        }
        if (cleaned)
        {
            CheckpointConfig force = new CheckpointConfig();
            force.setForce(true);
            _environment.checkpoint(force);
        }


        _environment.getConfig().setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "true");
    }

    private long getSizeOnDisk()
    {
        return _environment.getStats(null).getTotalLogSize();
    }

    private long getPersistentSizeLowThreshold()
    {
        return _persistentSizeLowThreshold;
    }

    private long getPersistentSizeHighThreshold()
    {
        return _persistentSizeHighThreshold;
    }

    private void setEnvironmentConfigProperties(EnvironmentConfig envConfig)
    {
        for (Map.Entry<String, String> configItem : _envConfigMap.entrySet())
        {
            LOGGER.debug("Setting EnvironmentConfig key " + configItem.getKey() + " to '" + configItem.getValue() + "'");
            envConfig.setConfigParam(configItem.getKey(), configItem.getValue());
        }
    }

    protected EnvironmentConfig createEnvironmentConfig()
    {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        setEnvironmentConfigProperties(envConfig);

        envConfig.setExceptionListener(new LoggingAsyncExceptionListener());

        return envConfig;
    }

    protected void closeEnvironmentSafely()
    {
        try
        {
            _environment.close();
        }
        catch (DatabaseException ex)
        {
            LOGGER.error("Exception closing store environment", ex);
        }
        catch (IllegalStateException ex)
        {
            LOGGER.error("Exception closing store environment", ex);
        }
    }


    private class LoggingAsyncExceptionListener implements ExceptionListener
    {
        @Override
        public void exceptionThrown(ExceptionEvent event)
        {
            LOGGER.error("Asynchronous exception thrown by BDB thread '"
                         + event.getThreadName() + "'", event.getException());
        }
    }
}
