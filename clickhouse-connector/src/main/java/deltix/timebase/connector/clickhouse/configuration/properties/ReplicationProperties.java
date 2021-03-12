package deltix.timebase.connector.clickhouse.configuration.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "replication")
public class ReplicationProperties {
    private List<String> streams;
    private int flushMessageCount = 10_000;
    private long flushTimeoutMs = 60_000;
    private long pollingIntervalMs = 60_000;


    public List<String> getStreams() {
        return streams;
    }

    public void setStreams(List<String> streams) {
        this.streams = streams;
    }

    public int getFlushMessageCount() {
        return flushMessageCount;
    }

    public void setFlushMessageCount(int flushMessageCount) {
        this.flushMessageCount = flushMessageCount;
    }

    public long getFlushTimeoutMs() {
        return flushTimeoutMs;
    }

    public void setFlushTimeoutMs(long flushTimeoutMs) {
        this.flushTimeoutMs = flushTimeoutMs;
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }

    public void setPollingIntervalMs(long pollingIntervalMs) {
        this.pollingIntervalMs = pollingIntervalMs;
    }
}
