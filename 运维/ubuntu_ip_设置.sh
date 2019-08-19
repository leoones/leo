vim /etc/network/interfaces

    auto lo
    ifact lo inet loopback
    auto ens160
    iface ens160 inet static
    address 192.168.1.103
    netmask 255.255.255.0
    gatway 192.168.1.1


vim /etc/NetworkManager/NetworkManager.conf

     [main]
      plugins=ifupdown,keyfile,ofono
      dns=dnsmasq

      [ifupdown]
      managed=true
