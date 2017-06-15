class proxy::base () {

    package { 'epel-release':
        ensure => installed,
    }

    package {
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

    package {
        'tox':
            ensure   => installed,
            provider => pip,
    }

}
