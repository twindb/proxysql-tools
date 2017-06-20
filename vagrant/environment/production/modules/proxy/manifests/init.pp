class proxy() {
    include proxy::proxysql_tools
    include proxy::base

    package { 'epel-release':
        ensure => installed,
    }

    package {
        'proxysql':
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
