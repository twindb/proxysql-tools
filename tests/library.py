import docker
import json
import socket
import time


def docker_client():
    return docker.from_env()


def docker_pull_image(image):
    """Pull the specified image using docker-py. This function will parse the
        result from docker-py and raise an exception if there is an error.

    :param str image: Name of the image to pull
    """
    client = docker_client()
    api = client.api

    response = api.pull(image)
    lines = [line for line in response.splitlines() if line]

    # The last line of the pull operation contains the overall result of the
    # pull operation.
    pull_result = json.loads(lines[-1])
    if "error" in pull_result:
        raise Exception("Could not pull {}: {}".format(image, pull_result["error"]))


def get_unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.bind(('localhost', 0))
    _, port = s.getsockname()
    s.close()

    return port


def is_port_reachable(host, port):
    s = socket.socket()
    try:
        s.connect((host, port))
        return True
    except socket.error:
        return False


def eventually(func, *args, **kwargs):
    retries = kwargs.pop('retries', 90)
    sleep_time = kwargs.pop('sleep_time', 0.5)
    assert_val = kwargs.pop('assert_val', False)

    last_ex = None
    for i in xrange(retries):
        if i > 0:
            time.sleep(sleep_time)

        try:
            val = func(*args, **kwargs)
            if assert_val:
                assert val
            return val
        except Exception as e:
            last_ex = e

    if not last_ex:
        last_ex = AssertionError('No result from %s' % func)

    if last_ex:
        raise last_ex
