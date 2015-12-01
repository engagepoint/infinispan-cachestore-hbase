package org.infinispan.loaders.hbase.util;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public class HBaseUtils {

    public static String getKeyFromResult(Result result) {
        List<KeyValue> l = result.list();

        // This assumes that the first keyValue will always
        // be the one that contains the actual row key. So we
        // don't need to iterate over all keyValue items.
        // We can always iterate over all KeyValues and output
        // a warning message if the key inferred for a given
        // KeyValue is different than the other KeyValues' keys.
        // This also assumes that the string representation of
        // the KeyValue has the key as the start of the string,
        // going all the way to the first "/".
        KeyValue keyValue = l.get(0);
        String keyValStr = keyValue.toString();
        int index = keyValStr.indexOf("/");

        return keyValStr.substring(0, index);
    }
}
