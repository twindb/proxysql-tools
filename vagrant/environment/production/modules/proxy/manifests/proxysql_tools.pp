class proxy::proxysql_tools () {

    package {
        'proxysql-tools':
            ensure   => installed,
            provider => pip,
            require  => [
                Package['python-pip'],
                Package['python-devel'],
            ]
    }

    file {
        '/etc/twindb':
            ensure => directory;
        "/etc/twindb/proxysql-tools.cfg":
            ensure  => present,
            content => template("proxy/proxysql-tools.cfg.erb"),
            require => File['/etc/twindb'],
    }
}
