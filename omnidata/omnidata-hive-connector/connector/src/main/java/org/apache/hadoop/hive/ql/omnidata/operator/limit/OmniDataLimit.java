package org.apache.hadoop.hive.ql.omnidata.operator.limit;

import java.util.OptionalLong;

/**
 * OmniData Limit
 *
 * @since 2022-02-18
 */
public class OmniDataLimit {
    public static OptionalLong getOmniDataLimit(long limit) {
        return OptionalLong.of(limit);
    }
}
