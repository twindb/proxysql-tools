def test__docker_works(debian_container):
    assert debian_container.status == 'running'

    container_config = debian_container.attrs['Config']

    cmd_output = debian_container.exec_run(cmd="hostname")
    hostname = cmd_output.strip()

    assert hostname == container_config['Hostname']
