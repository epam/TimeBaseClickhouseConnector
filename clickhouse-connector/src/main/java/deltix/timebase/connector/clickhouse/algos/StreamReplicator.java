package deltix.timebase.connector.clickhouse.algos;

import com.epam.deltix.gflog.api.*;
import com.epam.deltix.util.concurrent.UnavailableResourceException;
import com.epam.deltix.util.memory.MemoryDataInput;
import deltix.clickhouse.ClickhouseClient;
import deltix.clickhouse.schema.TableDeclaration;

import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.qsrv.hf.pub.codec.CodecFactory;
import com.epam.deltix.qsrv.hf.pub.codec.UnboundDecoder;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.tickdb.pub.SelectionOptions;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickCursor;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickStream;
import deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static deltix.timebase.connector.clickhouse.algos.SchemaProcessor.TIMESTAMP_COLUMN_NAME;

public class StreamReplicator implements Runnable {

    protected static final Log LOG = LogFactory.getLog(StreamReplicator.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneOffset.UTC);

    private final TickStream timebaseStream;
    private final ClickhouseClient clickhouseClient;
    private final ClickhouseProperties clickhouseProperties;
    private final Consumer<StreamReplicator> onStopped;
    private final int flushMessageCount;
    private final long flushTimeoutMs;

    private volatile boolean cancel = false;
    private long lastFlushTimestamp = 0;

    private UnboundTableWriter tableWriter = null;

    private final Object unblockingCursorLock = new Object();


    public StreamReplicator(TickStream timebaseStream,
                            ClickhouseClient clickhouseClient,
                            ClickhouseProperties clickhouseProperties,
                            int flushMessageCount,
                            long flushTimeoutMs,
                            Consumer<StreamReplicator> onStopped) {
        this.timebaseStream = timebaseStream;
        this.clickhouseClient = clickhouseClient;
        this.clickhouseProperties = clickhouseProperties;
        this.onStopped = onStopped;

        if (flushMessageCount <= 0)
            throw new IllegalArgumentException("Illegal flushMessageCount");
        if (flushTimeoutMs <= 0)
            throw new IllegalArgumentException("Illegal flushTimeoutMs");

        this.flushMessageCount = flushMessageCount;
        this.flushTimeoutMs = flushTimeoutMs;
    }

    @Override
    public void run() {
        try {
            SchemaProcessor schemaProcessor = new SchemaProcessor(timebaseStream, clickhouseClient, clickhouseProperties);

            LOG.info()
                    .append("Replication ")
                    .append(timebaseStream.getKey())
                    .append(": prepare target table schema.")
                    .commit();
            TableDeclaration clickhouseTable = schemaProcessor.prepareClickhouseTable();

            // find load start time, truncate target table if necessary
            Pair<Instant, Instant> timeInterval = getTimeInterval(clickhouseTable);
            long lastCompletelyProcessedTimestamp = Long.MIN_VALUE;

            if (timeInterval != null) {
                LOG.info()
                        .append("Table `")
                        .append(clickhouseTable.getTableIdentity().toString())
                        .append("` ")
                        .append("available data interval: ")
                        .append(formatToDateTime3(timeInterval.getLeft()))
                        .append(" - ")
                        .append(formatToDateTime3(timeInterval.getRight()))
                        .commit();

                truncateData(clickhouseTable, timeInterval.getRight());
                lastCompletelyProcessedTimestamp = timeInterval.getRight().toEpochMilli();
            }

            SelectionOptions selectionOptions = new SelectionOptions(true, true);

            MemoryDataInput in = new MemoryDataInput();
            RecordClassDescriptor[] descriptors;
            if (timebaseStream.isFixedType())
                descriptors = new RecordClassDescriptor[]{timebaseStream.getFixedType()};
            else
                descriptors = timebaseStream.getPolymorphicDescriptors();

            List<UnboundDecoder> decoders = Arrays.stream(descriptors).map(rd -> CodecFactory.COMPILED.createFixedUnboundDecoder(rd)).collect(Collectors.toList());

            tableWriter = new UnboundTableWriter(timebaseStream.getKey(), clickhouseClient,
                    clickhouseTable,  schemaProcessor.getColumnDeclarations(), decoders, in/*, 10_000, 5_000*/);

            try (TickCursor cursor = timebaseStream.select(
                    lastCompletelyProcessedTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : lastCompletelyProcessedTimestamp + 1,
                    selectionOptions)) {
                // making live cursor non-blocking
                cursor.setAvailabilityListener(() -> notifyDataAvailable());

                do {
                    if (cancel)
                        break;

                    if (tableWriter.getBatchMsgCount() > 0) { // we have messages in queue
                        if (tableWriter.getBatchMsgCount() >= flushMessageCount || // batch size reached
                                System.currentTimeMillis() >= lastFlushTimestamp + flushTimeoutMs) // flush interval reached
                            flush();
                    }

                    synchronized (unblockingCursorLock) {
                        try {
                            if (cursor.next())
                                tableWriter.send((RawMessage) cursor.getMessage(), cursor);
                            else
                                break;
                        } catch (UnavailableResourceException e) {
                            try {
                                synchronized (unblockingCursorLock) {
                                    long timeout = flushTimeoutMs - (System.currentTimeMillis() - lastFlushTimestamp);
                                    if (timeout > 0)
                                        unblockingCursorLock.wait(timeout);
                                }
                            } catch (InterruptedException ie) {
                                // continue
                            }
//                    } catch (CursorIsClosedException ie) {
//                        return;
//                    } catch (deltix.util.io.UncheckedIOException ex) {
//                        if (ex.getCause() instanceof EOFException)
//                            return;
//                        else
//                            throw ex;
                        }

                    }
                } while (true);
            }
            LOG.info()
                    .append("Replication ")
                    .append(timebaseStream.getKey())
                    .append(": read process finished. Stopping.")
                    .commit();

            //flush(); // slice might be incomplete
        } catch (SQLException e) {
            LOG.error()
                    .append("Replication ")
                    .append(timebaseStream.getKey())
                    .append(": unhandled exception during replication process. Stopping.")
                    .append(e)
                    .commit();
        } catch (Exception e) {
            LOG.error()
                    .append("Replication ")
                    .append(timebaseStream.getKey())
                    .append(": unhandled exception during replication process. Stopping.")
                    .append(e)
                    .commit();
        } finally {
            if (tableWriter != null)
                tableWriter.close();
            onStopped.accept(this);
            LOG.info()
                    .append("Replication ")
                    .append(timebaseStream.getKey())
                    .append(": stopped.")
                    .commit();
        }
    }

    public void stop() {
        cancel = true;
    }

    private void notifyDataAvailable() {
        synchronized (unblockingCursorLock) {
            unblockingCursorLock.notify();
        }
    }

    private void flush() throws SQLException {
        tableWriter.flush();
        lastFlushTimestamp = System.currentTimeMillis();
    }

    public TickStream getTimebaseStream() {
        return timebaseStream;
    }

    private Pair<Instant, Instant> getTimeInterval(TableDeclaration clickhouseTable) {
        final String minTimestampAlias = "minTimestamp";
        final String maxTimestampAlias = "maxTimestamp";
        String selectQuery = String.format("SELECT min(%s) as %s, max(%s) AS %s FROM %s",
                TIMESTAMP_COLUMN_NAME, minTimestampAlias, TIMESTAMP_COLUMN_NAME, maxTimestampAlias,
                clickhouseTable.getTableIdentity().toString());
        LOG.debug()
                .append(selectQuery)
                .commit();

        Pair<Instant, Instant> result = clickhouseClient.getJdbcTemplate().query(selectQuery, new ResultSetExtractor<Pair<Instant, Instant>>() {
            @Override
            public Pair<Instant, Instant> extractData(ResultSet rs) throws SQLException, DataAccessException {

                if (rs.next()) {
                    Timestamp minTimestamp = getTimestamp(rs, minTimestampAlias);
                    Timestamp maxTimestamp = getTimestamp(rs, maxTimestampAlias);

                    if (minTimestamp == null && maxTimestamp == null)
                        return null;

                    return Pair.of(minTimestamp.toInstant(), maxTimestamp.toInstant());
                }

                return null;
            }
        });

        return result;
    }

    private void truncateData(TableDeclaration clickhouseTable, Instant toTime) {
        LOG.info()
                .append("Table `")
                .append(clickhouseTable.getTableIdentity().toString())
                .append("` ")
                .append("truncate data to ")
                .append(formatToDateTime3(toTime))
                .commit();

        String deleteTailQuery = String.format("ALTER TABLE %s DELETE WHERE %s = toDateTime64('%s',9)",
                clickhouseTable.getTableIdentity().toString(), TIMESTAMP_COLUMN_NAME, formatToDateTime3(toTime));
        LOG.debug()
                .append(deleteTailQuery)
                .commit();

//        clickhouseClient.getJdbcTemplate().execute(deleteTailQuery);
    }

    private static String formatToDateTime3(Instant instant) {
        return DATE_TIME_FORMATTER.format(instant);
    }

    private static Timestamp getTimestamp(ResultSet resultSet, String columnName) throws SQLException {
        String timestampString = resultSet.getString(columnName);

        Timestamp timestamp = "0000-00-00 00:00:00".equals(timestampString) ||
                "0000-00-00 00:00:00.000".equals(timestampString) ||
                "0000-00-00 00:00:00.000000".equals(timestampString) ||
                "0000-00-00 00:00:00.000000000".equals(timestampString) ?
                null :
                resultSet.getTimestamp(columnName);

        return timestamp;
    }
}
