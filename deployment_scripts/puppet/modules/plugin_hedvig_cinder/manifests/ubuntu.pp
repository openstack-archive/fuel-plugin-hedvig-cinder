class plugin_hedvig_cinder::ubuntu
{
    file { "/usr/lib/python2.7/dist-packages/cinder/volume/drivers/hedvig/":
            ensure => directory,
    }

    package {'libboost-all-dev':
        ensure => installed ,
    }->

    package {'libsnappy-dev':
        ensure => installed ,
    }->

    package {'libgoogle-glog-dev':
        ensure => installed ,
    }->

    package {'hedvigscripts':
        ensure => installed ,
    }->

    ini_setting { 'cinder_conf_enabled_backends':
        ensure  => present,
        path    => '/etc/cinder/cinder.conf',
        section => 'DEFAULT',
        setting => 'enabled_backends',
        value => 'hedvig',
        before => Ini_setting['cinder_conf_volume_driver'],
    } ->

    ini_setting { 'cinder_conf_volume_driver':
        ensure  => present,
        path    => '/etc/cinder/cinder.conf',
        section => 'hedvig',
        setting => 'volume_driver',
        value => 'cinder.volume.drivers.hedvig.hedvig-cinder.HedvigISCSIDriver',
        before => Ini_setting['cinder_conf_hedvig_metadata_server'],
    } ->
    
    ini_setting { 'cinder_conf_hedvig_metadata_server':
        ensure  => present,
        path    => '/etc/cinder/cinder.conf',
        section => 'hedvig',
        setting => 'hedvig_metadata_server',
        value => $metadata_server,
    } ->

    service { $services:
        ensure => running,
    }->
    exec { "Create Cinder volume type \'${volume_type}\'":
        command => "bash -c 'source /root/openrc; cinder type-create ${volume_type}'",
        path    => ['/usr/bin', '/bin'],
        unless  => "bash -c 'source /root/openrc; cinder type-list |grep -q \" ${volume_type} \"'",
    }

    exec { "ldconfig /usr/local/lib":
        command => 'ldconfig /usr/local/lib',
        path    => ['/usr/local/sbin','/usr/local/bin','/usr/sbin','/usr/bin','/sbin','/bin'],
    } ->

    exec { "ReStart Cinder volume ":
        command => 'service cinder-volume restart',
        path    => ['/usr/local/sbin','/usr/local/bin','/usr/sbin','/usr/bin','/sbin','/bin'],
    }
}
