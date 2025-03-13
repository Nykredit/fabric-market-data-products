# Market Data Products Fabric
This repository is synced with market data products microsoft fabric workspaces. 

It contains the code and configurations for the workspace, as well as the pipelines for deploying to dev test and production.

![alt text](https://learn.microsoft.com/en-us/fabric/cicd/media/manage-deployment/git-build.png "Title")

Link to workspaces:

- [Dev Environment Workspace](https://app.fabric.microsoft.com/groups/e7787afa-5823-4d22-8ca2-af0f38d1a339)
- [Test Environment Workspace](https://app.fabric.microsoft.com/groups/cb5044bf-b47b-4a8b-939e-2fb21e235985)
- [Prod Environment Workspace](https://app.fabric.microsoft.com/groups/472e684d-212d-49c0-a3f1-0b2d0756dc19)

Link to feature workspaces:

- [Feature Workspace 1](https://app.fabric.microsoft.com/groups/a9b81e29-69e9-43e5-a605-81643ce56b09)
- [Feature Workspace 2](https://app.fabric.microsoft.com/groups/63831729-0c77-4bf0-8519-4118b7d522dd)

# Local Development
It is possible to develop and test the Python Spark jobs locally by using a dev container with Docker
Desktop and Visual Studio Code. 

## Setup Dev Container
1) First ensure you have WSL, Docker Desktop and Visual Studio Code instaled, use the guide
[here](https://wiki.tools.nykredit.it/pages/viewpage.action?spaceKey=SDP&title=Windows+Subsystem+for+Linux+%28WSL%29+in+Nykredit).

2) Ensure you have the Nykredit proxies configured for Docker. Modify your docker config to include the following:

```json
{
    "proxies": {
        "default": {
        "httpProxy": "http://httpproxy.nykreditnet.net:8080",
        "httpsProxy": "http://httpproxy.nykreditnet.net:8080",
        "noProxy": "localhost,127.0.0.1,.nykreditnet.net"
        }
    }
}
```
The docker config can usually be found in `C:\Users\<user>\.docker\config.json`.

3) In Docker Desktop create a Dev Environment that either clones the repository or points to your existing
git repository.

4) Start the dev environment and launch the Visual Studio Code from Docker Desktop. You should now be able
to do local development with Python integration, unit tests etc. in the dev container Visual Studio Code
instance.

## Running Unit Tests
Running the unit tests for the repository is quite simple, when the dev container is running.

1) Open a terminal and run the bash script to create symlinks needed for the test files:

```bash
./.github/scripts/create_testing_symlinks.sh
```

2) Run the python testing framework pytest in the root:

```bash
pytest .
```
