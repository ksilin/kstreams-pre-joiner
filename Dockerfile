# Build stage: no local JDK needed
FROM eclipse-temurin:21-jdk AS build
WORKDIR /work
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
RUN ./gradlew --no-daemon -q dependencies > /dev/null
COPY src src
RUN ./gradlew --no-daemon -q build -x test

FROM registry.access.redhat.com/ubi9/openjdk-21:1.23
ENV LANGUAGE='en_US:en'
COPY --from=build --chown=185 /work/build/quarkus-app/lib/ /deployments/lib/
COPY --from=build --chown=185 /work/build/quarkus-app/*.jar /deployments/
COPY --from=build --chown=185 /work/build/quarkus-app/app/ /deployments/app/
COPY --from=build --chown=185 /work/build/quarkus-app/quarkus/ /deployments/quarkus/
EXPOSE 8080
USER 185
ENV JAVA_OPTS_APPEND="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
ENV JAVA_APP_JAR="/deployments/quarkus-run.jar"
ENTRYPOINT [ "/opt/jboss/container/java/run/run-java.sh" ]
