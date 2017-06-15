class percona::repository {

    case $::osfamily {
        'redhat': {
            $releasever = "6"
            $basearch = $hardwaremodel
            yumrepo {
                "percona":
                    descr       => "Percona",
                    enabled     => 1,
                    baseurl     => "http://repo.percona.com/centos/$releasever/os/$basearch/",
                    gpgcheck    => 0;
            }
        }
        'debian': {
            include apt

            apt::source { 'percona':
                comment  => 'Percona releases, stable',
                location => 'http://repo.percona.com/apt',
                release  => 'jessie',
                repos    => 'main',
                key      => {
                    'id'     => '4D1BB29D63D98E422B2113B19334A25F8507EFA5',
                    'server' => 'keys.gnupg.net',
                },
                include  => {
                    'src' => false,
                    'deb' => true,
                },
            }
        }
        default: {
            err "Unsupported OS"
        }
    }


}
