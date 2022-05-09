FROM mcr.microsoft.com/azure-functions/java:4-java11-core-tools

COPY samples-azure-functions/build/azure-functions/azure-functions-sample/ /home/src/azure-functions-sample/
WORKDIR /home/src/azure-functions-sample
EXPOSE 7071
RUN func -version

CMD func start
