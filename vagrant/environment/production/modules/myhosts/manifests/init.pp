class myhosts {
	host {
		"node1":
			ensure	=> "present",
			ip 	=> "192.168.90.2";
		"node2":
			ensure	=> "present",
			ip	=> "192.168.90.3";
		"node3":
			ensure	=> "present",
			ip	=> "192.168.90.4";
        "proxysql":
            ensure	=> "present",
            ip	=> "192.168.90.5";
	}
}
