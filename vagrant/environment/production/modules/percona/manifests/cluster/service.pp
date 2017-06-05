class percona::cluster::service ($ensure="running", $bootstrap=false) {

    if ($bootstrap == true) {
        # $service_start = "systemctl start mysql@bootstrap.service"
        $service_start = "/etc/init.d/mysql bootstrap-pxc"
    }

	service {
        "mysql":
            enable  => true,
            ensure  => $ensure,
            subscribe => File['/etc/my.cnf'],
            require => Package['MySQL-server'],
            start => $service_start,
    }

    mysql_user { 'root@192.168.90.%':
        ensure        => 'present',
        password_hash => '*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B', # root
    }

    mysql_user { 'proxysql_user@%':
        ensure        => 'present',
        password_hash => '*BF27B4C7AAD278126E228AA8427806E870F64F39',
    }
    mysql_user { 'monitor@%':
        ensure        => 'present',
        password_hash => '*E1A383418F81A65DB753CF477200945A1A5BA77D',
    }
}
