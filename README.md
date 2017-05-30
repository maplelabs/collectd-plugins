# Collectd Python Plugins

###### Download plugins to /opt/collectd/plugins/

### Plugins Configs:

#### apache_perf
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "apache_perf"

    <Module apache_perf>
        interval "300"
        port "APACHE PORT NUMBER"
        location "LOCATION e.g. server-status"
        secure "False"
    </Module>
</Plugin>
```

#### apache_trans
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "apache_trans"

    <Module apache_trans>
        accesslog "PATH of access.log e.g. /var/log/apache2/access.log"
        name "apache_trans"
        interval "100"
    </Module>
</Plugin>
```
#### jvm_stats
```xml
<Plugin python>
        ModulePath "/opt/collectd/plugins"
        LogTraces true
        Interactive false
        Import "jvm_stats"

        <Module jvm_stats>
            interval "300"
            process "JAVA PROCESS NAME, e.g. elasticsearch"
        </Module>
</Plugin>
```
#### cpu_static
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "cpu_static"

    <Module cpu_static>
        interval "10"
    </Module>
</Plugin>
```
#### cpu_util
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "cpu_util"

    <Module cpu_util>
        interval "10"
    </Module>
</Plugin>
```
#### tcp_stats
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "tcp_stats"

    <Module tcp_stats>
        interval "300"
    </Module>
</Plugin>
```
#### nic_stats
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "nic_stats"

    <Module nic_stats>
        interval "300"
    </Module>
</Plugin>
```
#### ram_util
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "ram_util"

    <Module ram_util>
        interval "300"
    </Module>
</Plugin>
```
#### disk_stat
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "disk_stat"

    <Module disk_stat>
        interval "300"
    </Module>
</Plugin>
```
#### mysql
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "mysql"

    <Module mysql>
        interval "300"
        host "MYSQL HOST e.g. localhost"
        user "MYSQL USERNAME"
        password "MYSQL PASSWORD"
    </Module>
</Plugin>
```
#### libvirt_interface
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "libvirt_interface"

    <Module libvirt_interface>
        interval "10"
    </Module>
</Plugin>
```
#### libirt_disk
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "libvirt_disk"

    <Module libvirt_disk>
        interval "10"
    </Module>
</Plugin>
```
#### libvirt_compute
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "libvirt_compute"

    <Module libvirt_compute>
        interval "10"
    </Module>
</Plugin>
```
#### libvirt_static
```xml
<Plugin python>
    ModulePath "/opt/collectd/plugins"
    LogTraces true
    Interactive false
    Import "libvirt_static"

    <Module libvirt_static>
        interval "10"
    </Module>
</Plugin>
```

