package org.apache.flink.connector.jdbc.internal.options;


import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * @author hst
 * @create 2024-11-28 15:10
 * @Description:
 */
public class JdbcFilterOptions implements Serializable {

    private final String filter;

    private JdbcFilterOptions(String filter) {
        this.filter = filter;
    }

    public Optional<String> getFilter() {
        return Optional.ofNullable(filter);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JdbcFilterOptions) {
            JdbcFilterOptions options = (JdbcFilterOptions) o;
            return Objects.equals(filter,options.filter);
        } else {
            return false;
        }
    }

    public static class Builder {
        protected String filter;

        public Builder setFilter(String filter) {
            this.filter = filter;
            return this;
        }

        public JdbcFilterOptions build() {
            return new JdbcFilterOptions(filter);
        }
    }





}
