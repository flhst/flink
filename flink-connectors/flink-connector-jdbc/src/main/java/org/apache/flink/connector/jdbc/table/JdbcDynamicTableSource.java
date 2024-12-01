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
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcFilterOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/** A {@link DynamicTableSource} for JDBC. */
// getScanRuntimeProvider 数据查找
// getLookupRuntimeProvider 数据遍历
// lookup 简单理解为根据提交查找，不必读取整个表，并且可以在需要时从（可能不断变化的）外部表中延迟获取各个值
// scan 在运行时扫描来自外部存储系统的所有行
@Internal
public class JdbcDynamicTableSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown {

    private final JdbcConnectorOptions options;
    private final JdbcReadOptions readOptions;
    private final JdbcFilterOptions filterOptions;
    private final int lookupMaxRetryTimes;
    @Nullable private final LookupCache cache;
    private DataType physicalRowDataType;
    private final String dialectName;
    private long limit = -1;

    public JdbcDynamicTableSource(
            JdbcConnectorOptions options,
            JdbcReadOptions readOptions,
            JdbcFilterOptions filterOptions,
            int lookupMaxRetryTimes,
            @Nullable LookupCache cache,
            DataType physicalRowDataType) {
        this.options = options;
        this.readOptions = readOptions;
        this.filterOptions = filterOptions;
        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
        this.cache = cache;
        this.physicalRowDataType = physicalRowDataType;
        this.dialectName = options.getDialect().dialectName();
    }

    // 数据查找
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // JDBC only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
        }
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        JdbcRowDataLookupFunction lookupFunction =
                new JdbcRowDataLookupFunction(
                        options,
                        lookupMaxRetryTimes,
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        keyNames,
                        rowType);
        if (cache != null) {
            return PartialCachingLookupProvider.of(lookupFunction, cache);
        } else {
            return LookupFunctionProvider.of(lookupFunction);
        }
    }

    // 数据遍历
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // 初始化Builder
        final JdbcRowDataInputFormat.Builder builder =
                JdbcRowDataInputFormat.builder()
                        .setDrivername(options.getDriverName())
                        .setDBUrl(options.getDbURL())
                        .setUsername(options.getUsername().orElse(null))
                        .setPassword(options.getPassword().orElse(null))
                        .setAutoCommit(readOptions.getAutoCommit());

        // 设置FetchSize
        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }
        // 获取方言类型，调用对应的DialectFactory，构建select语句
        final JdbcDialect dialect = options.getDialect();
        String query =
                dialect.getSelectFromStatement(
                        options.getTableName(),
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        new String[0]);

        Optional<String> filter = filterOptions.getFilter();
        if (filter.isPresent() && !StringUtils.isEmpty(filter.get())) {
            if ("Oracle".equals(dialectName)) {
                query = query + " WHERE " + filter.get().replace("\"", "'");
            } else {
                query = query + " WHERE " + filter.get();
            }
        }

        // 分区条件构建
        if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            builder.setParametersProvider(
                    new JdbcNumericBetweenParametersProvider(lowerBound, upperBound)
                            .ofBatchNum(numPartitions));
            query +=
                    (query.contains("WHERE") ? " AND " : " WHERE ")
                            + dialect.quoteIdentifier(readOptions.getPartitionColumnName().get())
                            + " BETWEEN ? AND ?";
        }

        // limit 条件构建
        if (limit >= 0) {
            query = String.format("%s %s", query, dialect.getLimitClause(limit));
        }
        builder.setQuery(query);
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        // 外部数据类型转成Flink类型转换器
        builder.setRowConverter(dialect.getRowConverter(rowType));
        // 设置数据类型信息，把符合类型展开
        builder.setRowDataTypeInfo(
                runtimeProviderContext.createTypeInformation(physicalRowDataType));

        return InputFormatProvider.of(builder.build());
    }

    // 数据变更类型，insertOnly
    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public DynamicTableSource copy() {
        return new JdbcDynamicTableSource(
                options,
                readOptions,
                filterOptions,
                lookupMaxRetryTimes,
                cache,
                physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcDynamicTableSource)) {
            return false;
        }
        JdbcDynamicTableSource that = (JdbcDynamicTableSource) o;
        return Objects.equals(options, that.options)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(lookupMaxRetryTimes, that.lookupMaxRetryTimes)
                && Objects.equals(cache, that.cache)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(dialectName, that.dialectName)
                && Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                options,
                readOptions,
                lookupMaxRetryTimes,
                cache,
                physicalRowDataType,
                dialectName,
                limit);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }
}
