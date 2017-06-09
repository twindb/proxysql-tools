class proxy::docker () {

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
