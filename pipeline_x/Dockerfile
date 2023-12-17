FROM python:3.10-slim

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagster-webserver and dagster-daemon, and to load the DagsterInstance

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-aws \
    pandas \
    dagster-k8s

# Add repository code

WORKDIR /opt/dagster/app
RUN mkdir data
COPY . /opt/dagster/app/pipeline_x

## Run dagster gRPC server on port 4000
#
#EXPOSE 4000
#
## CMD allows this to be overridden from run launchers or executors that want
## to run other commands against your repository
#CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipeline_x"]
