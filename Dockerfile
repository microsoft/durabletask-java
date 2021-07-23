FROM gradle:jdk11 as builder

COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build

FROM openjdk:11-jre-slim
EXPOSE 4001 4000 8080
COPY --from=builder /home/gradle/src/samples/build/distributions/samples-0.1.0.tar /app/
WORKDIR /app
RUN tar -xvf samples-0.1.0.tar
WORKDIR /app/samples-0.1.0
CMD bin/samples io.durabletask.samples.WebApplication
