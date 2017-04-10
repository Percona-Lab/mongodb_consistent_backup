%define debug_package	%{nil}
%define	bin_name	mongodb-consistent-backup
%define log_dir		/var/log/mongodb-consistent-backup
%define data_dir	/var/lib/mongodb-consistent-backup
%define run_user	mongodb_consistent_backup
%define run_group	mongodb_consistent_backup

Name:		mongodb_consistent_backup
Version:	%{version}
Release:	1%{?dist}
Summary:	MongoDB Consistent Backup Tool

Group:		Software/Database
License:	Apache License Version 2.0
URL:		https://github.com/Percona-Lab/mongodb_consistent_backup
Source0:	%{bin_name}
Source1:	%{bin_name}.yml
Source2:	LICENSE
Source3:	README.rst
Prefix:		/usr

# Use CentOS SCL python27 (https://www.softwarecollections.org/en/scls/rhscl/python27/) on CentOS 6 (RHEL6 untested)
# On build host: 'yum install python27-python python27-python-devel python27-python-virtualenv gcc'
%{?el6:Requires: python27-python}
%{?el6:BuildRequires: python27-python python27-python-devel python27-python-virtualenv gcc}

# Use base python/virtualenv, which should be 2.7 on CentOS/RHEL 7
# On build host: 'yum install python python-devel python-virtualenv gcc'
%{?el7:Requires: python >= 2.7}
%{?el7:BuildRequires: python >= 2.7 python-devel >= 2.7 python-virtualenv gcc}


%description
Tool for getting consistent backups from MongoDB Clusters and ReplicaSet


%install
mkdir -p %{buildroot}%{_sysconfdir}/cron.d %{buildroot}%{prefix}/bin %{buildroot}/usr/share/%{name}

install -m 0755 %{SOURCE0} %{buildroot}%{prefix}/bin/%{bin_name}
install -m 0644 %{SOURCE1} %{buildroot}/usr/share/%{name}/%{bin_name}.example.yml
install -m 0644 %{SOURCE1} %{buildroot}%{_sysconfdir}/%{bin_name}.yml
install -m 0644 %{SOURCE2} %{buildroot}/usr/share/%{name}/LICENSE
install -m 0644 %{SOURCE3} %{buildroot}/usr/share/%{name}/README.rst


# Generate cron.d file:
%{__cat} <<EOF >%{buildroot}%{_sysconfdir}/cron.d/%{name}
### Uncomment and adjust time to enable backups (default time below is 00:00 every day):
#
#0 0 * * *	%{run_user}	/usr/bin/mongodb-consistent-backup --config=/etc/mongodb-consistent-backup.yml >/dev/null 2>&1
EOF

# Change /etc config file to use rpm paths for logs and data
sed -i \
	-e s@log_dir:\ /tmp@log_dir:\ %{log_dir}@g \
	-e s@location:\ /opt/mongodb/backup@location:\ %{data_dir}@g \
	%{buildroot}%{_sysconfdir}/%{bin_name}.yml


%pre
/usr/bin/getent group %{run_group} >/dev/null 2>&1 || /usr/sbin/groupadd -r %{run_group}
/usr/bin/getent passwd %{run_user} >/dev/null 2>&1 || /usr/sbin/useradd -r -d %{data_dir} -g %{run_group} -s /sbin/nologin %{run_user}


%post
[ ! -d %{data_dir} ] && mkdir -m 0750 -p %{data_dir}
[ ! -d %{log_dir} ] && mkdir -m 0755 -p %{log_dir}
chown %{run_user}:%{run_group} %{data_dir} %{log_dir}


%files
%{_sysconfdir}/%{bin_name}.yml
%{_sysconfdir}/cron.d/%{name}
%{prefix}/bin/%{bin_name}
%{prefix}/share/%{name}/%{bin_name}.example.yml
%{prefix}/share/%{name}/LICENSE
%{prefix}/share/%{name}/README.rst


%changelog

