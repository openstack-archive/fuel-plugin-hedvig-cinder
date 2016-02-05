class plugin_hedvig_cinder
{
  if($::operatingsystem == 'Ubuntu') {
    include plugin_hedvig_cinder::ubuntu
  }else {
    include plugin_hedvig_cinder::centos
  }
}
