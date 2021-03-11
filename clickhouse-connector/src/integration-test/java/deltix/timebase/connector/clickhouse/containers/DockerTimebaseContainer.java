package deltix.timebase.connector.clickhouse.containers;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;

@Testcontainers
public class DockerTimebaseContainer {
    private static final Log LOG = LogFactory.getLog(DockerTimebaseContainer.class);
    private static final String DOCKER_IMAGE_NAME = "registry-dev.deltixhub.com/quantserver.docker/timebase/server:6.0";
    private static final String TIMEBASE_SERIAL_NUMBER_SYSTEM_VAR_NAME = "TIMEBASE_SERIAL_NUMBER";
    private static volatile DockerTimebaseContainer instance;
    @Container
    private static GenericContainer<?> container;

    private DockerTimebaseContainer() {
        LOG.info("Initialization...");
        container = new GenericContainer<>(DOCKER_IMAGE_NAME)
                .withExposedPorts(8011)
                .withCopyFileToContainer(attachLicense(), "/timebase-server/inst.properties");

        LOG.info().append("Container parameters: ").append(container.toString()).commit();
        LOG.info("Container starting..");
        container.start();
        LOG.info("Container successfully started.");
    }

    private static MountableFile attachLicense() {
        try {
            LOG.info("Start license file creation.");
            Path license = Files.createTempFile("inst", ".properties");

            final String serialNumber = System.getenv(TIMEBASE_SERIAL_NUMBER_SYSTEM_VAR_NAME);
            final String fileContent = String.format("serial=%s", serialNumber);

            LOG.info("Start writing to the temp system file.");
            Files.write(license, fileContent.getBytes());
            LOG.info().append("License file created successfully. ").append("Location: ").append(license.toAbsolutePath().toString()).commit();

            return MountableFile.forHostPath(license);
        } catch (Exception ex) {
            LOG.info().append("Exception during creating temp system file. ").append(ex).commit();
            throw new IllegalArgumentException(ex);
        }
    }

    public static DockerTimebaseContainer getInstance() {
        if (instance == null) {
            synchronized (DockerTimebaseContainer.class) {
                if (instance == null)
                    instance = new DockerTimebaseContainer();
            }
        }
        return instance;
    }

    public GenericContainer<?> getContainer() {
        return container;
    }
}
