class percona::cluster ($mysql_version="5.5", $enable=true, $ensure=running, $bootstrap=false) {

    include percona::cluster::packages

	Class['percona::cluster::packages'] -> Class['percona::cluster::config'] ->  Class['percona::cluster::service']

	class {
        'percona::cluster::service':
            bootstrap => $bootstrap,
			ensure 	  => $ensure,
    }
}
