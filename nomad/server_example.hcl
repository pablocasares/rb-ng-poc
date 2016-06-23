log_level = "DEBUG"

region = "global"
datacenter = "dc1"
name = "server2"

data_dir = "/tmp/nomad/"
bind_addr = "10.0.150.104"

leave_on_interrupt = true
leave_on_terminate = true

server {
    enabled = true
    data_dir = "/tmp/nomad/server/"
    num_schedulers = 1 #Cores num
    bootstrap_expect = 3
    start_join = ["10.0.150.103", "10.0.150.104", "10.0.150.105"]
    retry_join = ["10.0.150.103", "10.0.150.104", "10.0.150.105"]
}
