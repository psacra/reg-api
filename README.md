# Registration API Gateway

This software acts as a Gateway to the Registration API allowing to manage automatically ingestion of assets included in a STAC catalogue entry.

NOTE: This software is not operational, it is to be considered a demo software application illustrating how to interface with a OGC Records catalogue and how to implement a small internal API. Documentation and support is thus limited.

## Deployment


### Install docker on the machine

First install the docker software

```
#On RHEL9:
dnf install yum-utils
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin docker-buildx-plugin
```

Then configure it to start

```
systemctl start docker
systemctl enable docker
```

### Create a user to run the application

Create a user for the S3 and registration API and provide access to the stagein folder to it.

Note that this user will own the stagein, assets and STAC folders

```
groupadd -g 1012 reg-api
useradd -u 1012 -g 1012 -G docker reg-api
```

### Install and configure the application

Checkout this repository in the home of the reg-api user

```
git clone https://github.com/EOEPCA/reg-api
```

Build the docker container as reg-api user via

```
cd reg-api
./build_docker
```

Edit the configuration file according to your installation paths (see comments in the configuration file template)

```
cp cfg/onf.yaml.template cfg/conf.yaml
vi cfg/conf.yaml
```

Ensure the folders you have specified in the conf.yaml file exists and are owned by the reg-api user

```
#The following command needs to be run as root
mkdir /mystore/stagein /mystore/assets /mystore/stac
chown -R reg-api: /mystore/stagein /mystore/assets /mystore/stac
```

Then initialize the authorization DB
```
./initdb_docker
```

At last start the container service

```
./start_docker
```

## Manage the application

The `reg-api` script will allow you to perform basic management operations like creating users, creating collections and associating users and buckets to collections.

To know how to use it, you can access the help on

```
docker exec -it reg-api-server reg-api --help
```

## API Usage

The documentation on API usage is available, after the execution of the service, at the `/reg-api/docs/` address.

## Development

The script `run_development` can be used during development to enable fastapi debugging and allow modifications of the `bin` and `src` directories to propagate within the docker container execution
