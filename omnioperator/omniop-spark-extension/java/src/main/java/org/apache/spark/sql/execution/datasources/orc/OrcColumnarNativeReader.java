/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package org.apache.spark.sql.execution.datasources.orc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader.Options;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcColumnarNativeReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrcColumnarNativeReader.class);

    public static Options buildOptions(Configuration conf, long start, long length) {
        TypeDescription schema =
            TypeDescription.fromString(OrcConf.MAPRED_INPUT_SCHEMA.getString(conf));
        Options options = new Options(conf)
        .range(start, length)
        .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
        .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
        .tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf));
        if (schema != null) {
            options.schema(schema);
        } else {
            // TODO
            LOGGER.error("TODO: null schema should support");
        }
        options.include(OrcInputFormat.parseInclude(schema,
            OrcConf.INCLUDE_COLUMNS.getString(conf)));
        String kryoSarg = OrcConf.KRYO_SARG.getString(conf);
        String sargColumns = OrcConf.SARG_COLUMNS.getString(conf);
        if (kryoSarg != null && sargColumns != null) {
            byte[] sargBytes = Base64.decodeBase64(kryoSarg);
            SearchArgument sarg =
                new Kryo().readObject(new Input(sargBytes), SearchArgumentImpl.class);
            options.searchArgument(sarg, sargColumns.split(","));
            sarg.getExpression().toString();
        }
        return options;
    }
}
