package deltix.timebase.connector.clickhouse.configuration.beans;

import deltix.qsrv.hf.spi.conn.Disconnectable;
import deltix.qsrv.hf.tickdb.pub.DXTickDB;
import deltix.qsrv.hf.tickdb.pub.TickDBFactory;
import deltix.timebase.connector.clickhouse.configuration.properties.TimebaseProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class TimeBaseConnectorService {
    private final DXTickDB tickDB;

    @Autowired
    public TimeBaseConnectorService(TimebaseProperties settings) {
        TickDBFactory.setApplicationName("axa-backend");
        if (settings.getUsername() != null && !settings.getUsername().isEmpty())
            tickDB = TickDBFactory.createFromUrl(settings.getUrl(), settings.getUsername(), settings.getPassword());
        else
            tickDB = TickDBFactory.createFromUrl(settings.getUrl());

        tickDB.open(true);
   }

    @PreDestroy
    private void destroy() {
        tickDB.close();
    }

    @Bean
    public DXTickDB getTickDB() {
        return tickDB;
    }

    public boolean isConnected() {
        if (tickDB instanceof Disconnectable)
            return ((Disconnectable) tickDB).isConnected();

        // failover in case DXTickDB does not implement
        return true;
    }
}
