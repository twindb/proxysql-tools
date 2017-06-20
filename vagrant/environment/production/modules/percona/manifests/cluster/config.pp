class percona::cluster::config  ($perconaserverid=undef) {
    $mysql_version = $percona::cluster::mysql_version

    file {
        "/etc/my.cnf":
            ensure  => present,
            content => template("percona/cluster/my.cnf.erb"),
    }

    class { selinux:
        mode => 'permissive',
        type => 'targeted',
    }

}
