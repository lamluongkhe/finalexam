FROM docker.io/maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /workspace
COPY . .
RUN mvn -q clean install -DskipTests


FROM apache/karaf:4.4.6

COPY --from=build \
/workspace/person-feature/target/person-feature-1.0-SNAPSHOT.kar \
/opt/apache-karaf/deploy/



EXPOSE 8181 8101
CMD ["karaf", "run"]
