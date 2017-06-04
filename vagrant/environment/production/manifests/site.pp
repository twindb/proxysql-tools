node node1 {
    include percona::repository
    include myhosts

	Class['percona::repository'] -> Class['percona::cluster']


	class {
		'percona::cluster':
            mysql_version   => "5.6",
            enable          => true,
            ensure          => "running",
            bootstrap 	    => true
	}

	class {'percona::cluster::config': perconaserverid => "1" }

}

node node2 {
	include percona::repository
	include myhosts

	Class['percona::repository'] -> Class['percona::cluster']

	class {
        'percona::cluster':
            mysql_version   => "5.6",
            enable          => "true",
            ensure          => "running"
    }
	class {'percona::cluster::config': perconaserverid => "2" }
}

node node3 {
	include percona::repository
	include myhosts

	Class['percona::repository'] -> Class['percona::cluster']

	class {
		'percona::cluster':
            mysql_version   => "5.6",
            enable          => "true",
            ensure          => "running"
	}
	class {'percona::cluster::config': perconaserverid => "3" }
}

node proxysql {
	include percona::repository
    class { 'proxy':

    }
}
