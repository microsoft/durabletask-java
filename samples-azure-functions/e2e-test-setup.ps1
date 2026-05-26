# Installing PowerShell: https://docs.microsoft.com/powershell/scripting/install/installing-powershell

param(
	[Parameter(Mandatory=$true)]
	[string]$DockerfilePath,
	[string]$ImageName="dfapp",
	[string]$ContainerName="app",
	[switch]$NoSetup=$false,
	[switch]$NoValidation=$false,
	[string]$AzuriteVersion="3.34.0",
	[int]$Sleep=30
)

$ErrorActionPreference = "Stop"

if ($NoSetup -eq $false) {
	# Build the docker image first, since that's the most critical step
	Write-Host "Building sample app Docker container from '$DockerfilePath'..." -ForegroundColor Yellow
	docker build -f $DockerfilePath -t $ImageName --progress plain .

	# Next, download and start the Azurite emulator Docker image
	Write-Host "Pulling down the mcr.microsoft.com/azure-storage/azurite:$AzuriteVersion image..." -ForegroundColor Yellow
	docker pull "mcr.microsoft.com/azure-storage/azurite:${AzuriteVersion}"

	Write-Host "Starting Azurite storage emulator using default ports..." -ForegroundColor Yellow
	docker run --name 'azurite' -p 10000:10000 -p 10001:10001 -p 10002:10002 -d "mcr.microsoft.com/azure-storage/azurite:${AzuriteVersion}"

	# Finally, start up the smoke test container, which will connect to the Azurite container
	docker run --name $ContainerName -p 8080:80 -it --add-host=host.docker.internal:host-gateway -d `
		--env 'AzureWebJobsStorage=UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://host.docker.internal' `
		--env 'WEBSITE_HOSTNAME=localhost:8080' `
		$ImageName
}

if ($sleep -gt  0) {
	# The container needs a bit more time before it can start receiving requests
	Write-Host "Sleeping for $Sleep seconds to let the container finish initializing..." -ForegroundColor Yellow
	Start-Sleep -Seconds $Sleep
}

# Check to see what containers are running
docker ps