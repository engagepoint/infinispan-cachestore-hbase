package org.infinispan.loaders.hbase.configuration;

import java.util.HashMap;
import java.util.Map;


public enum Attribute {
   // must be first
   UNKNOWN(null),
   AUTO_CREATE_TABLE("auto-create-table"),
   ENTRY_COLUMN_FAMILY("entry-column-family"),
   ENTRY_TABLE("entry-table"),
   ENTRY_VALUE_FIELD("entry-value-field"),
   EXPIRATION_COLUMN_FAMILY("expiration-column-family"),
   EXPIRATION_TABLE("expiration-table"),
   EXPIRATION_VALUE_FIELD("expiration-value-field"),
   SHARED_TABLE("shared-table"),
   HBASE_ZOOKEEPER_QUORUM("hbase-zookeeper-quorum"),
   HBASE_ZOOKEEPER_CLIENT_PORT("hbase-zookeeper-client-port");

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   /**
    * Get the local name of this element.
    *
    * @return the local name
    */
   public String getLocalName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.getLocalName();
         if (name != null) {
            map.put(name, attribute);
         }
      }
      attributes = map;
   }

   public static Attribute forName(final String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }
}
