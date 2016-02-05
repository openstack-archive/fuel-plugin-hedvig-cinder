$plugin_settings = hiera('fuel-plugin-hedvig-cinder')
$services = ['cinder-volume']
$metadata_server = $plugin_settings['hedvig_metadata_server']
$volume_type = "hedvig"
class {'plugin_hedvig_cinder': }
