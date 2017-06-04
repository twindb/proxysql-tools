class percona::cluster::packages {

    if $percona::cluster::mysql_version == "5.5" {
        $ps_ver="55"
    } elsif $percona::cluster::mysql_version == "5.6" {
        info("Congrats ! Using 5.6 !!")
        $ps_ver="56"
    }

    package {
        'mariadb-libs':
            require => [
                Package['postfix'],
            ],
            ensure => absent;
        "postfix":
            ensure => absent;
    }


    package {
        "MySQL-server":
            alias => "MySQL-server",
            name => "Percona-XtraDB-Cluster-server-$ps_ver.$hardwaremodel",
            require => [
                Yumrepo['percona'],
                Package['mariadb-libs'],
            ],
            ensure => "installed";
        "qpress":
             require => Yumrepo['percona'],
             ensure => "present";
        "rsync":
            ensure => "present";

    }
}
