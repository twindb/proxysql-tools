import docker
import json
import os
import pprint
import pytest


CONTAINERS_FOR_TESTING_LABEL = 'pytest_docker'


def _docker_client():
    return docker.from_env()


def pytest_runtest_logreport(report):
    """Process a test setup/call/teardown report relating to the respective phase of executing a test."""
    if report.failed:
        client = _docker_client()
        api = client.api

        test_containers = api.containers(all=True, filters={"label": CONTAINERS_FOR_TESTING_LABEL})

        for container in test_containers:
            log_lines = [
                ("docker inspect {!r}:".format(container['Id'])),
                (pprint.pformat(api.inspect_container(container['Id']))),
                ("docker logs {!r}:".format(container['Id'])),
                (api.logs(container['Id'])),
            ]
            report.longrepr.addsection('docker logs', os.linesep.join(log_lines))


def pull_image(image):
    """Pull the specified image using docker-py. This function will parse the result from docker-py and raise an
    exception if there is an error.

    :param str image: Name of the image to pull
    """
    client = _docker_client()
    response = client.images.pull(image)
    lines = [line for line in response.splitlines() if line]

    # The last line of the pull operation contains the overall result of the pull operation.
    pull_result = json.loads(lines[-1])
    if "error" in pull_result:
        raise Exception("Could not pull {}: {}".format(image, pull_result["error"]))


@pytest.yield_fixture
def debian_container():
    client = _docker_client()
    api = client.api

    container = api.create_container(image='debian:8', labels=[CONTAINERS_FOR_TESTING_LABEL],
                                     command='/bin/sleep 36000')
    api.start(container['Id'])

    container_info = client.containers.get(container['Id'])

    yield container_info

    api.remove_container(container=container['Id'], force=True)
