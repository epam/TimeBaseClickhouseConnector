FROM adoptopenjdk/openjdk11
COPY ../build/docker/clickhouse-connector/clickhouse-connector /clickhouse-connector
RUN echo "#!/bin/sh \n \n java -Dgflog.config=/configuration/gflog.xml 	-Dorg.springframework.boot.logging.LoggingSystem=none 	-Dspring.config.name=application,clickhouse-connector,constants,timebase,clickhouse 	-Dspring.config.location=classpath:/,file:/configuration/ \${JAVA_OPTS} 	-cp /clickhouse-connector/BOOT-INF/classes:/clickhouse-connector/BOOT-INF/lib/*:/clickhouse-connector org.springframework.boot.loader.JarLauncher "\$@" " > /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
