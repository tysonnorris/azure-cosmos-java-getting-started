FROM adoptopenjdk/maven-openjdk11:latest

COPY src ./src
COPY pom.xml ./pom.xml

#cache ddependencies
RUN mvn dependency:resolve-plugins dependency:resolve dependency:go-offline clean install exec:help

#script to constantly run test
COPY runtest.sh ./runtest.sh

CMD sh ./runtest.sh
