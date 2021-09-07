package deltix.timebase.connector.clickhouse.services;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickDB;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickDB;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickStream;
import deltix.timebase.connector.clickhouse.algos.StreamReplicator;
import deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import deltix.timebase.connector.clickhouse.configuration.properties.ReplicationProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ReplicatorService {

    private static final Log LOG = LogFactory.getLog(ReplicatorService.class);

    private final TickDB tickDb;
    private final ClickhouseClient clickhouseClient;
    private final ClickhouseProperties clickhouseProperties;
    private final ReplicationProperties replicationProperties;
    private final Map<StreamReplicator, Thread> replicators = new ConcurrentHashMap<>();
    private Timer timer;

    @Autowired
    public ReplicatorService(ClickhouseClient clickhouseClient, DXTickDB tickDb,
                             ClickhouseProperties clickhouseProperties, ReplicationProperties replicationProperties) {
        this.clickhouseClient = clickhouseClient;
        this.tickDb = tickDb;
        this.clickhouseProperties = clickhouseProperties;
        this.replicationProperties = replicationProperties;
    }

    private static String createRegexFromGlob(String glob) {
        StringBuilder out = new StringBuilder("^");
        for (int i = 0; i < glob.length(); ++i) {
            final char c = glob.charAt(i);
            switch (c) {
                case '*':
                    out.append(".*");
                    break;
                case '?':
                    out.append('.');
                    break;
                case '.':
                    out.append("\\.");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                default:
                    out.append(c);
            }
        }
        out.append('$');
        return out.toString();
    }

    @PostConstruct
    private void init() {
        if (replicationProperties.getPollingIntervalMs() > 0)
            startTimer();
        else
            startReplicators();
    }

    @PreDestroy
    public void destroy() {

        LOG.info().append("Dispose ReplicatorService.").commit();
        if (timer != null){
            timer.cancel();;
        }

        // first stop all threads
        for (Map.Entry<StreamReplicator, Thread> entry : replicators.entrySet())
            entry.getKey().stop();

        // then wait for completion
        for (Map.Entry<StreamReplicator, Thread> entry : replicators.entrySet()) {
            Thread replicatorThread = entry.getValue();

            try {
                LOG.info()
                        .append("Joining thread '")
                        .append(replicatorThread.getName())
                        .append("'")
                        .commit();

                replicatorThread.join();
            } catch (InterruptedException e) {
                // log and continue
                LOG.info()
                        .append("Thread '")
                        .append(replicatorThread.getName())
                        .append("' interrupted.")
                        .commit();
            }
        }

        LOG.info().append("ReplicatorService disposed.").commit();
    }

    private void startTimer() {
        timer = new java.util.Timer("long polling");
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                startReplicators();
            }
        }, 0, replicationProperties.getPollingIntervalMs());
    }


    public void startReplicators() {
        getStreams()
                .stream()
                .forEach(stream -> {
                    StreamReplicator replicator = new StreamReplicator(stream, clickhouseClient, clickhouseProperties,
                            replicationProperties.getFlushMessageCount(),
                            replicationProperties.getFlushTimeoutMs(), streamReplicator -> onReplicatorStopped(streamReplicator));
                    Thread replicatorThread = new Thread(replicator, String.format("Stream replicator '%s'", stream.getKey()));

                    replicators.put(replicator, replicatorThread);

                    replicatorThread.start();
                });
    }

    private Thread onReplicatorStopped(StreamReplicator streamReplicator) {
        return replicators.remove(streamReplicator);
    }

    private List<TickStream> getStreams() {
        try {
            Set<TickStream> result = new HashSet<>();
            final TickStream[] tickStreams = tickDb.listStreams();
            final List<Pattern> patterns = replicationProperties.getStreams().stream().map(stream -> Pattern.compile(createRegexFromGlob(stream))).collect(Collectors.toList());
            for (TickStream stream : tickStreams) {
                for (Pattern pattern : patterns) {
                    Matcher m = pattern.matcher(stream.getKey());
                    if (m.matches())
                        result.add(stream);
                }
            }
            result.removeAll(replicators.keySet().stream().map(replicator -> replicator.getTimebaseStream()).collect(Collectors.toList()));
            return result.stream().collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error()
                    .append(e)
                    .commit();
            return Collections.emptyList();
        }

    }

}
