package io.github.reugn.micrometer.aerospike;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.util.StringEscapeUtils;
import io.micrometer.core.lang.Nullable;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

public class AerospikeNamingConvention implements NamingConvention {

    private static final Pattern BLACKLISTED_CHARS = Pattern.compile("[{}():,=\\[\\]]");

    private final NamingConvention delegate;

    public AerospikeNamingConvention() {
        this(NamingConvention.snakeCase);
    }

    public AerospikeNamingConvention(NamingConvention delegate) {
        this.delegate = delegate;
    }

    private String format(String name) {
        String normalized = StringEscapeUtils.escapeJson(name);
        return BLACKLISTED_CHARS.matcher(normalized).replaceAll("_");
    }

    @Override
    @Nonnull
    public String name(@Nonnull String name, @Nonnull Meter.Type type, @Nullable String baseUnit) {
        return format(delegate.name(name, type, baseUnit));
    }

    @Override
    @Nonnull
    public String tagKey(@Nonnull String key) {
        return format(delegate.tagKey(key));
    }

    @Override
    @Nonnull
    public String tagValue(@Nonnull String value) {
        return format(delegate.tagValue(value));
    }
}
