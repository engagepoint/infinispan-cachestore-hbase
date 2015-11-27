package org.infinispan.loaders.hbase.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.*;

import javax.xml.stream.XMLStreamException;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;


@Namespaces({
        @Namespace(uri = "urn:infinispan:config:store:hbase:7.2", root = "hbase-store"),
        @Namespace(root = "hbase-store")
})
public class HBaseCacheStoreConfigurationParser72 implements ConfigurationParser {

    public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
        ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

        Element element = Element.forName(reader.getLocalName());
        switch (element) {
            case HBASE_STORE: {
                parseHBaseStore(reader, builder.persistence(), holder.getClassLoader());
                break;
            }
            default: {
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    }

    private void parseHBaseStore(final XMLExtendedStreamReader reader, PersistenceConfigurationBuilder loadersBuilder, ClassLoader classLoader) throws XMLStreamException {
        HBaseCacheStoreConfigurationBuilder builder = new HBaseCacheStoreConfigurationBuilder(loadersBuilder);
        parseHBaseStoreAttributes(reader, builder);
        loadersBuilder.addStore(builder);
    }

    private void parseHBaseStoreAttributes(XMLExtendedStreamReader reader, HBaseCacheStoreConfigurationBuilder builder) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case AUTO_CREATE_TABLE: {
                    builder.autoCreateTable(Boolean.parseBoolean(value));
                    break;
                }
                case ENTRY_COLUMN_FAMILY: {
                    builder.entryColumnFamily(value);
                    break;
                }
                case ENTRY_TABLE: {
                    builder.entryTable(value);
                    break;
                }
                case ENTRY_VALUE_FIELD: {
                    builder.entryValueField(value);
                    break;
                }
                case EXPIRATION_COLUMN_FAMILY: {
                    builder.expirationColumnFamily(value);
                    break;
                }
                case EXPIRATION_TABLE: {
                    builder.expirationTable(value);
                    break;
                }
                case EXPIRATION_VALUE_FIELD: {
                    builder.expirationValueField(value);
                    break;
                }
                case SHARED_TABLE: {
                    builder.sharedTable(Boolean.parseBoolean(value));
                    break;
                }
                case HBASE_ZOOKEEPER_QUORUM: {
                    builder.hbaseZookeeperQuorumHost(value);
                    break;
                }
                case HBASE_ZOOKEEPER_CLIENT_PORT: {
                    builder.hbaseZookeeperClientPort(Integer.parseInt(value));
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        if (reader.hasNext()) {
            reader.nextTag();
        }
    }

    public Namespace[] getNamespaces() {
        return ParseUtils.getNamespaceAnnotations(getClass());
    }

}
