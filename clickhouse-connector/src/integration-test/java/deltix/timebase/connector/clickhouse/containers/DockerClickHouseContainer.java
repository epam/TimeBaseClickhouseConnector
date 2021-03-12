package deltix.timebase.connector.clickhouse.containers;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class DockerClickHouseContainer {
    private static final Log LOG = LogFactory.getLog(DockerClickHouseContainer.class);

    private static volatile DockerClickHouseContainer instance;

    @Container
    private static ClickHouseContainer container;
    private static final String DOCKER_IMAGE_NAME = "yandex/clickhouse-server";

    private DockerClickHouseContainer() {
        LOG.info("Initialization...");
        container = new ClickHouseContainer(DOCKER_IMAGE_NAME);

        LOG.info().append("Container parameters: ").append(container.toString()).commit();
        LOG.info("Container starting..");

        container.start();

        LOG.info("Container successfully started.");
    }

    public static DockerClickHouseContainer getInstance() {
        if (instance == null) {
            synchronized (DockerClickHouseContainer.class) {
                if (instance == null)
                    instance = new DockerClickHouseContainer();
            }
        }
        return instance;
    }

    public ClickHouseContainer getContainer() {
        return container;
    }
}
