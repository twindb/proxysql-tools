class proxy() {
    include proxy::proxysql_tools
    # include proxy::docker

    package { 'epel-release':
        ensure => installed,
    }

    package {
        'proxysql':
            ensure  => installed;
        'mysql':
            ensure  => installed;
        'python-pip':
            ensure  => installed,
            require => Package['epel-release'];
        'python-devel':
            ensure  => installed;
        'openssl-devel':
            ensure  => installed;
        'vim':
            ensure  => installed;
    }


    service {
        'proxysql':
            ensure  => running,
            require => Package['proxysql'],
    }

    file {
        '/root/.my.cnf':
            content => '
[client]
user=admin
password=admin
socket=/tmp/proxysql_admin.sock'
    }

}
