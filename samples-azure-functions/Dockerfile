FROM mcr.microsoft.com/azure-functions/java:4-java11

COPY samples-azure-functions/build/azure-functions/azure-functions-sample/ /home/site/wwwroot/
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true
