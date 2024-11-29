/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcFilterOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.DRIVER;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.JDBC_FILTER_QUERY;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_MISSING_KEY;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.MAX_RETRY_TIMEOUT;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_AUTO_COMMIT;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_FETCH_SIZE;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_PARTITION_COLUMN;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_PARTITION_LOWER_BOUND;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_PARTITION_NUM;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_PARTITION_UPPER_BOUND;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.USERNAME;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource} and {@link
 * JdbcDynamicTableSink}.
 */
@Internal
public class JdbcDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "jdbc";

    // 构建sink动态表
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config, context.getClassLoader());
        validateDataTypeWithJdbcDialect(
                context.getPhysicalRowDataType(), config.get(URL), context.getClassLoader());
        // 获取JDBC连接选项
        JdbcConnectorOptions jdbcOptions = getJdbcOptions(config, context.getClassLoader());

        return new JdbcDynamicTableSink(
                jdbcOptions,
                getJdbcExecutionOptions(config),
                getJdbcDmlOptions(
                        jdbcOptions,
                        context.getPhysicalRowDataType(),
                        context.getPrimaryKeyIndexes()),
                context.getPhysicalRowDataType());
    }

    // 构建source动态表
    // Context： DDL配置等上下文信息
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // 创建TableFactoryHelper对象（传入当前对象和上下文）
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 获取配置选项
        final ReadableConfig config = helper.getOptions();

        // 验证配置选项的有效性
        helper.validate();
        // 进一步验证配置选项
        validateConfigOptions(config, context.getClassLoader());
        // 验证数据类型，验证数据类型与JDBC方言的一致性
        validateDataTypeWithJdbcDialect(
                context.getPhysicalRowDataType(), config.get(URL), context.getClassLoader());
        // 创建 JdbcDynamicTableSource 实例:
        // 1、使用 getJdbcOptions 和 getJdbcReadOptions 方法获取JDBC选项。
        // 2、获取 LookupOptions.MAX_RETRIES 配置项。
        // 3、从配置中获取 查询缓存 的配置项。
        // 3、获取物理行数据类型。
        // 4、创建并返回一个新的 JdbcDynamicTableSource 实例。
        return new JdbcDynamicTableSource(
                getJdbcOptions(helper.getOptions(), context.getClassLoader()),
                getJdbcReadOptions(helper.getOptions()),
                getJdbcFilterOptions(helper.getOptions()),
                helper.getOptions().get(LookupOptions.MAX_RETRIES),
                getLookupCache(config),
                context.getPhysicalRowDataType());
    }

    // 验证数据类型，验证数据类型与JDBC方言的一致性
    private static void validateDataTypeWithJdbcDialect(
            DataType dataType, String url, ClassLoader classLoader) {
        final JdbcDialect dialect = JdbcDialectLoader.load(url, classLoader);
        dialect.validate((RowType) dataType.getLogicalType());
    }

    // 从配置对象 ReadableConfig 中读取必要的数据库连接参数，
    // 并构建一个 JdbcConnectorOptions 对象。
    //  1、从 readableConfig 中获取数据库URL。
    //  2、使用 JdbcConnectorOptions.Builder 构建一个连接选项的构建器。
    //  3、设置类加载器、数据库URL、表名、方言、并行度和连接超时时间。
    //  4、如果配置中提供了驱动程序名称、用户名和密码，则分别设置这些参数。
    //  5、最后调用 builder.build() 方法构建并返回 JdbcConnectorOptions 对象。
    private JdbcConnectorOptions getJdbcOptions(
            ReadableConfig readableConfig, ClassLoader classLoader) {
        final String url = readableConfig.get(URL);
        final JdbcConnectorOptions.Builder builder =
                JdbcConnectorOptions.builder()
                        .setClassLoader(classLoader)
                        .setDBUrl(url)
                        .setTableName(readableConfig.get(TABLE_NAME))
                        .setDialect(JdbcDialectLoader.load(url, classLoader))
                        .setParallelism(readableConfig.getOptional(SINK_PARALLELISM).orElse(null))
                        .setConnectionCheckTimeoutSeconds(
                                (int) readableConfig.get(MAX_RETRY_TIMEOUT).getSeconds());

        readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    // 从配置中读取分区列名、分区上界、分区下界、分区数、自动提交、查询批大小等参数，
    private JdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
        final Optional<String> partitionColumnName =
                readableConfig.getOptional(SCAN_PARTITION_COLUMN);
        final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
        if (partitionColumnName.isPresent()) {
            builder.setPartitionColumnName(partitionColumnName.get());
            builder.setPartitionLowerBound(readableConfig.get(SCAN_PARTITION_LOWER_BOUND));
            builder.setPartitionUpperBound(readableConfig.get(SCAN_PARTITION_UPPER_BOUND));
            builder.setNumPartitions(readableConfig.get(SCAN_PARTITION_NUM));
        }
        readableConfig.getOptional(SCAN_FETCH_SIZE).ifPresent(builder::setFetchSize);
        builder.setAutoCommit(readableConfig.get(SCAN_AUTO_COMMIT));
        return builder.build();
    }

    private JdbcFilterOptions getJdbcFilterOptions(ReadableConfig config) {
        final JdbcFilterOptions.Builder builder = new JdbcFilterOptions.Builder();
        builder.setFilter(config.get(JDBC_FILTER_QUERY));
        return builder.build();
    }

    // 获取JDBC批量写入选项
    // 设置批量写入的最大行数、批量写入的时间间隔、最大重试次数，
    // 构建并返回一个JdbcExecutionOptions对象。
    private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
        final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
        builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
        return builder.build();
    }

    // 获取JDBC DML写入选项
    // 提取主键字段名：根据 primaryKeyIndexes 数组，从 dataType 中提取对应的字段名，存储在 keyFields 数组中。
    // 构建 JdbcDmlOptions 对象：使用 JdbcDmlOptions.builder() 构建对象，并设置表名、方言、字段名和主键字段名
    private JdbcDmlOptions getJdbcDmlOptions(
            JdbcConnectorOptions jdbcOptions, DataType dataType, int[] primaryKeyIndexes) {

        String[] keyFields =
                Arrays.stream(primaryKeyIndexes)
                        .mapToObj(i -> DataType.getFieldNames(dataType).get(i))
                        .toArray(String[]::new);

        return JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(DataType.getFieldNames(dataType).toArray(new String[0]))
                .withKeyFields(keyFields.length > 0 ? keyFields : null)
                .build();
    }

    // 从配置中获取 查询缓存 的配置项。
    // 查询缓存可以存储之前查询的结果，
    // 当再次进行相同的查询时，可以直接从缓存中获取结果，
    // 而不需要重新执行数据库查询。
    // 这可以显著提高查询性能，减少数据库的负载。
    @Nullable
    private LookupCache getLookupCache(ReadableConfig tableOptions) {
        LookupCache cache = null;
        // Legacy cache options
        if (tableOptions.get(LOOKUP_CACHE_MAX_ROWS) > 0
                && tableOptions.get(LOOKUP_CACHE_TTL).compareTo(Duration.ZERO) > 0) {
            cache =
                    DefaultLookupCache.newBuilder()
                            .maximumSize(tableOptions.get(LOOKUP_CACHE_MAX_ROWS))
                            .expireAfterWrite(tableOptions.get(LOOKUP_CACHE_TTL))
                            .cacheMissingKey(tableOptions.get(LOOKUP_CACHE_MISSING_KEY))
                            .build();
        }
        if (tableOptions
                .get(LookupOptions.CACHE_TYPE)
                .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        }
        return cache;
    }

    // factory唯一标识
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    // 必选参数列表
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    // 可选参数列表
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(DRIVER);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_AUTO_COMMIT);
        optionalOptions.add(JDBC_FILTER_QUERY);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        optionalOptions.add(LOOKUP_CACHE_MISSING_KEY);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(MAX_RETRY_TIMEOUT);
        optionalOptions.add(LookupOptions.CACHE_TYPE);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_MAX_ROWS);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        optionalOptions.add(LookupOptions.MAX_RETRIES);
        return optionalOptions;
    }

    // 不影响程序运行拓扑结构的参数列表
    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        URL,
                        TABLE_NAME,
                        USERNAME,
                        PASSWORD,
                        DRIVER,
                        SINK_BUFFER_FLUSH_MAX_ROWS,
                        SINK_BUFFER_FLUSH_INTERVAL,
                        SINK_MAX_RETRIES,
                        MAX_RETRY_TIMEOUT,
                        SCAN_FETCH_SIZE,
                        SCAN_AUTO_COMMIT)
                .collect(Collectors.toSet());
    }

    // 作用: 进一步验证配置选项的高级有效性。
    // 具体实现: validateConfigOptions 方法通常会进行更详细的验证，
    // 例如检查配置选项是否符合特定的业务逻辑、是否与当前环境兼容等。
    // 这个方法可能会涉及到更多的业务逻辑和环境依赖。
    // 示例: 检查配置中的JDBC驱动是否可用、数据类型是否支持等。
    private void validateConfigOptions(ReadableConfig config, ClassLoader classLoader) {
        String jdbcUrl = config.get(URL);
        JdbcDialectLoader.load(jdbcUrl, classLoader);

        checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

        checkAllOrNone(
                config,
                new ConfigOption[] {
                    SCAN_PARTITION_COLUMN,
                    SCAN_PARTITION_NUM,
                    SCAN_PARTITION_LOWER_BOUND,
                    SCAN_PARTITION_UPPER_BOUND
                });

        if (config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent()
                && config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
            long lowerBound = config.get(SCAN_PARTITION_LOWER_BOUND);
            long upperBound = config.get(SCAN_PARTITION_UPPER_BOUND);
            if (lowerBound > upperBound) {
                throw new IllegalArgumentException(
                        String.format(
                                "'%s'='%s' must not be larger than '%s'='%s'.",
                                SCAN_PARTITION_LOWER_BOUND.key(),
                                lowerBound,
                                SCAN_PARTITION_UPPER_BOUND.key(),
                                upperBound));
            }
        }

        checkAllOrNone(config, new ConfigOption[] {LOOKUP_CACHE_MAX_ROWS, LOOKUP_CACHE_TTL});

        if (config.get(LOOKUP_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            LOOKUP_MAX_RETRIES.key(), config.get(LOOKUP_MAX_RETRIES)));
        }

        if (config.get(SINK_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
        }

        if (config.get(MAX_RETRY_TIMEOUT).getSeconds() <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option must be in second granularity and shouldn't be smaller than 1 second, but is %s.",
                            MAX_RETRY_TIMEOUT.key(),
                            config.get(
                                    ConfigOptions.key(MAX_RETRY_TIMEOUT.key())
                                            .stringType()
                                            .noDefaultValue())));
        }
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }
}
