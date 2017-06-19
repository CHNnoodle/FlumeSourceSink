package com.xjgreat;

import org.apache.flume.serialization.DecodeErrorPolicy;

public class SpoolDirectoryPathSourceConfigurationConstants {
    public static final String SPOOL_DIRECTORY = "spoolDir";
    public static final String SPOOLED_FILE_SUFFIX = "fileSuffix";
    public static final String DEFAULT_SPOOLED_FILE_SUFFIX = ".COMPLETED";

    public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
    public static final String DEFAULT_FILENAME_HEADER_KEY = "file";
    public static final String FILENAME_HEADER = "fileHeader";
    public static final boolean DEFAULT_FILE_HEADER = false;

    public static final String BASENAME_HEADER_KEY = "basenameHeaderKey";
    public static final String DEFAULT_BASENAME_HEADER_KEY = "basename";
    public static final String BASENAME_HEADER = "basenameHeader";
    public static final boolean DEFAULT_BASENAME_HEADER = false;

    public static final String PATHNAME_HEADER_KEY = "basePathHeaderKey";
    public static final String DEFAULT_PATHNAME_HEADER_KEY = "basepathname";
    public static final String PATHNAME_HEADER = "basePathHeader";
    public static final boolean DEFAULT_PATHNAME_HEADER = false;

    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;
    /** @deprecated */
    @Deprecated
    public static final String BUFFER_MAX_LINES = "bufferMaxLines";
    /** @deprecated */
    @Deprecated
    public static final int DEFAULT_BUFFER_MAX_LINES = 100;
    /** @deprecated */
    @Deprecated
    public static final String BUFFER_MAX_LINE_LENGTH = "bufferMaxLineLength";
    /** @deprecated */
    @Deprecated
    public static final int DEFAULT_BUFFER_MAX_LINE_LENGTH = 5000;
    public static final String IGNORE_PAT = "ignorePattern";
    public static final String DEFAULT_IGNORE_PAT = "^$";
    public static final String TRACKER_DIR = "trackerDir";
    public static final String DEFAULT_TRACKER_DIR = ".flumespool";
    public static final String DESERIALIZER = "deserializer";
    public static final String DEFAULT_DESERIALIZER = "LINE";
    public static final String DELETE_POLICY = "deletePolicy";
    public static final String DEFAULT_DELETE_POLICY = "never";
    public static final String INPUT_CHARSET = "inputCharset";
    public static final String DEFAULT_INPUT_CHARSET = "UTF-8";
    public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
    public static final String DEFAULT_DECODE_ERROR_POLICY;
    public static final String MAX_BACKOFF = "maxBackoff";
    public static final Integer DEFAULT_MAX_BACKOFF;
    public static final String CONSUME_ORDER = "consumeOrder";
    public static final SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder DEFAULT_CONSUME_ORDER;

    public SpoolDirectoryPathSourceConfigurationConstants() {
    }

    static {
        DEFAULT_DECODE_ERROR_POLICY = DecodeErrorPolicy.FAIL.name();
        DEFAULT_MAX_BACKOFF = Integer.valueOf(4000);
        DEFAULT_CONSUME_ORDER = SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder.OLDEST;
    }

    public static enum ConsumeOrder {
        OLDEST,
        YOUNGEST,
        RANDOM;

        private ConsumeOrder() {
        }
    }
}
