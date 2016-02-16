class plugin_hedvig_cinder::ubuntu
{
    file { "/usr/lib/python2.7/dist-packages/cinder/volume/drivers/hedvig/":
            ensure => directory,
    }

    file { "/usr/lib/python2.7/dist-packages/_hedvigpyc.so":
            source => 'puppet:///modules/plugin_hedvig_cinder/pythonlibs/_hedvigpyc.so',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/local/lib/libhedvig.so":
            source => 'puppet:///modules/plugin_hedvig_cinder/libs/libhedvig.so',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }
    
    file { "/usr/local/lib/libcrcutil.so":
            source => 'puppet:///modules/plugin_hedvig_cinder/libs/libcrcutil.so',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/local/lib/libthrift-0.9.0.so":
            source => 'puppet:///modules/plugin_hedvig_cinder/libs/libthrift-0.9.0.so',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/local/lib/libthriftnb-0.9.0.so":
            source => 'puppet:///modules/plugin_hedvig_cinder/libs/libthriftnb-0.9.0.so',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/local/lib/libunwind.so.8":
            source => 'puppet:///modules/plugin_hedvig_cinder/libs/libunwind.so.8',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/lib/python2.7/dist-packages/cinder/volume/drivers/hedvig/hedvig-cinder.py":
            source => 'puppet:///modules/plugin_hedvig_cinder/python/hedvig-cinder.py',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/lib/python2.7/dist-packages/cinder/volume/drivers/hedvig/hedvigpyc.py":
            source => 'puppet:///modules/plugin_hedvig_cinder/python/hedvigpyc.py',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/lib/python2.7/dist-packages/cinder/volume/drivers/hedvig/__init__.py":
            source => 'puppet:///modules/plugin_hedvig_cinder/python/__init__.py',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/lib/python2.7/dist-packages/cinder/volume/drivers/hedvig/HedvigConfig.py":
            source => 'puppet:///modules/plugin_hedvig_cinder/python/HedvigConfig.py',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/lib/python2.7/dist-packages/cinder/volume/drivers/hedvig/Helper.py":
            source => 'puppet:///modules/plugin_hedvig_cinder/python/Helper.py',
            mode  => '644',
            owner => 'root',
            group => 'root',
    }

    file { "/usr/lib/python2.7/dist-packages/cinder/volume/drivers/hedvig/HedvigOpCb.py":
            source => 'puppet:///modules/plugin_hedvig_cinder/python/HedvigOpCb.py',
            mode  => '644',
            owner => 'root',
            group => 'root',
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
