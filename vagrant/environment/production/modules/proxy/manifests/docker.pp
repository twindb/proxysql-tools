class proxy::docker () {

    include proxy::base

    package {
        'docker':
            ensure   => installed,
    }

    service {
        'docker':
            ensure  => running,
            require => Package['docker'],
    }

}
