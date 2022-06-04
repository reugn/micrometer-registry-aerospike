package io.github.reugn.micrometer.aerospike.client;

public class MicrometerPolicy {

    public static final MicrometerPolicy DEFAULT = new MicrometerPolicy();

    private final boolean meterMethodCalls;
    private final boolean meterMethodTime;
    private final boolean meterErrors;

    public MicrometerPolicy() {
        this(true, true, true);
    }

    public MicrometerPolicy(
            boolean meterMethodCalls,
            boolean meterMethodTime,
            boolean meterErrors
    ) {
        this.meterMethodCalls = meterMethodCalls;
        this.meterMethodTime = meterMethodTime;
        this.meterErrors = meterErrors;
    }

    public boolean isMeterMethodCalls() {
        return meterMethodCalls;
    }

    public boolean isMeterMethodTime() {
        return meterMethodTime;
    }

    public boolean isMeterErrors() {
        return meterErrors;
    }

    public static class Builder {
        private boolean meterMethodCalls;
        private boolean meterMethodTime;
        private boolean meterErrors;

        public Builder meterMethodCalls(boolean meterMethodCalls) {
            this.meterMethodCalls = meterMethodCalls;
            return this;
        }

        public Builder meterMethodTime(boolean meterMethodTime) {
            this.meterMethodTime = meterMethodTime;
            return this;
        }

        public Builder meterErrors(boolean meterErrors) {
            this.meterErrors = meterErrors;
            return this;
        }

        public MicrometerPolicy build() {
            return new MicrometerPolicy(
                    meterMethodCalls,
                    meterMethodTime,
                    meterErrors
            );
        }
    }
}
