package org.apache.qpid.server.configuration.store;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.configuration.ConfigurationEntry;
import org.apache.qpid.server.configuration.ConfigurationEntryStore;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.test.utils.TestFileUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class JsonConfigurationEntryStoreTest extends ConfigurationEntryStoreTestCase
{
    private File _storeFile;
    private ObjectMapper _objectMapper;

    @Override
    public void setUp() throws Exception
    {
        _objectMapper = new ObjectMapper();
        _objectMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception
    {
        _storeFile.delete();
        super.tearDown();
    }

    @Override
    protected ConfigurationEntryStore createStore(UUID brokerId, Map<String, Object> brokerAttributes) throws Exception
    {
        _storeFile = createStoreFile(brokerId, brokerAttributes);
        JsonConfigurationEntryStore store = new JsonConfigurationEntryStore(_storeFile.getAbsolutePath(), null);
        return store;
    }

    private File createStoreFile(UUID brokerId, Map<String, Object> brokerAttributes) throws IOException,
            JsonGenerationException, JsonMappingException
    {
        Map<String, Object> brokerObjectMap = new HashMap<String, Object>();
        brokerObjectMap.put(Broker.ID, brokerId);
        brokerObjectMap.put("@type", Broker.class.getSimpleName());
        brokerObjectMap.putAll(brokerAttributes);

        StringWriter sw = new StringWriter();
        _objectMapper.writeValue(sw, brokerObjectMap);

        String brokerJson = sw.toString();

        return TestFileUtils.createTempFile(this, ".json", brokerJson);
    }

    @Override
    protected void addConfiguration(UUID id, String type, Map<String, Object> attributes)
    {
        ConfigurationEntryStore store = getStore();
        store.save(new ConfigurationEntry(id, type, attributes, Collections.<UUID> emptySet(), store));
    }

    public void testAttributeIsResolvedFromSystemProperties()
    {
        String aclLocation = "path/to/acl/" + getTestName();
        setTestSystemProperty("my.test.property", aclLocation);

        ConfigurationEntryStore store = getStore();
        ConfigurationEntry brokerConfigEntry = store.getRootEntry();
        Map<String, Object> attributes = new HashMap<String, Object>(brokerConfigEntry.getAttributes());
        attributes.put(Broker.ACL_FILE, "${my.test.property}");
        ConfigurationEntry updatedBrokerEntry = new ConfigurationEntry(brokerConfigEntry.getId(), Broker.class.getSimpleName(),
                attributes, brokerConfigEntry.getChildrenIds(), store);
        store.save(updatedBrokerEntry);

        JsonConfigurationEntryStore store2 = new JsonConfigurationEntryStore(_storeFile.getAbsolutePath(), null);

        assertEquals("Unresolved ACL value", aclLocation, store2.getRootEntry().getAttributes().get(Broker.ACL_FILE));
    }

    public void testCreateEmptyStore()
    {
        File file = TestFileUtils.createTempFile(this, ".json");
        try
        {
            new JsonConfigurationEntryStore(file.getAbsolutePath(), null);
            fail("Cannot create a new store without initial store");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }
    }

    public void testCreateFromExistingLocation() throws Exception
    {
        UUID brokerId = UUID.randomUUID();
        Map<String, Object> brokerAttributes = new HashMap<String, Object>();
        brokerAttributes.put(Broker.NAME, getTestName());
        File file = createStoreFile(brokerId, brokerAttributes);

        JsonConfigurationEntryStore store = new JsonConfigurationEntryStore(file.getAbsolutePath(), null);
        ConfigurationEntry root = store.getRootEntry();
        assertNotNull("Root entry is not found", root);
        assertEquals("Unexpected root entry", brokerId, root.getId());
        Map<String, Object> attributes = root.getAttributes();
        assertNotNull("Attributes not found", attributes);
        assertEquals("Unexpected number of attriburtes", 1, attributes.size());
        assertEquals("Unexpected name attribute", getTestName(), attributes.get(Broker.NAME));
    }

    public void testCreateFromInitialStore() throws Exception
    {
        UUID brokerId = UUID.randomUUID();
        Map<String, Object> brokerAttributes = new HashMap<String, Object>();
        brokerAttributes.put(Broker.NAME, getTestName());
        File initialStoreFile = createStoreFile(brokerId, brokerAttributes);

        JsonConfigurationEntryStore initialStore = new JsonConfigurationEntryStore(initialStoreFile.getAbsolutePath(), null);

        File storeFile = TestFileUtils.createTempFile(this, ".json");
        JsonConfigurationEntryStore store = new JsonConfigurationEntryStore(storeFile.getAbsolutePath(), initialStore);

        ConfigurationEntry root = store.getRootEntry();
        assertNotNull("Root entry is not found", root);
        assertEquals("Unexpected root entry", brokerId, root.getId());
        Map<String, Object> attributes = root.getAttributes();
        assertNotNull("Attributes not found", attributes);
        assertEquals("Unexpected number of attriburtes", 1, attributes.size());
        assertEquals("Unexpected name attribute", getTestName(), attributes.get(Broker.NAME));
    }

}
