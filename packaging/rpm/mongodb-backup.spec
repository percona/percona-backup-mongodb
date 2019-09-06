%undefine _missing_build_ids_terminate_build
%global debug_package %{nil}
%{!?with_systemd:%global systemd 0}
%{?el7:          %global systemd 1}
%{?el8:          %global systemd 1}


Name:  percona-backup-mongodb
Version: 1.0
Release: 1%{?dist}
Summary: MongoDB backup tool

Group:  Applications/Databases
License: ASL 2.0
URL:  https://github.com/percona/percona-backup-mongodb
Source0: percona-backup-mongodb-%{version}.tar.gz

BuildRequires: golang make git
Requires(pre): /usr/sbin/useradd, /usr/bin/getent
Requires(postun): /usr/sbin/userdel
%if 0%{?systemd}
BuildRequires:  systemd
BuildRequires:  pkgconfig(systemd)
Requires(post):   systemd
Requires(preun):  systemd
Requires(postun): systemd
%else
Requires(post):   /sbin/chkconfig
Requires(preun):  /sbin/chkconfig
Requires(preun):  /sbin/service
%endif

%description
Percona Backup for MongoDB is a distributed, low-impact solution for achieving consistent backups of MongoDB Sharded Clusters and Replica Sets.


%prep
%setup -q -n percona-backup-mongodb-%{version}


%build
cd ../
export PATH=/usr/local/go/bin:${PATH}
export GOROOT="/usr/local/go/"
export GOPATH=$(pwd)/
export PATH="/usr/local/go/bin:$PATH:$GOPATH"
export GOBINPATH="/usr/local/go/bin"
mkdir -p src/github.com/percona/
mv percona-backup-mongodb-%{version} src/github.com/percona/percona-backup-mongodb
ln -s src/github.com/percona/percona-backup-mongodb percona-backup-mongodb-%{version}
cd src/ && go build github.com/percona/percona-backup-mongodb/cmd/pbm && go build github.com/percona/percona-backup-mongodb/cmd/pbm-agent 
cd %{_builddir}


%install
rm -rf $RPM_BUILD_ROOT
install -m 755 -d $RPM_BUILD_ROOT/%{_bindir}
cd ../
export PATH=/usr/local/go/bin:${PATH}
export GOROOT="/usr/local/go/"
export GOPATH=$(pwd)/
export PATH="/usr/local/go/bin:$PATH:$GOPATH"
export GOBINPATH="/usr/local/go/bin"
cd src/
cp pbm-agent $RPM_BUILD_ROOT/%{_bindir}/
cp pbm $RPM_BUILD_ROOT/%{_bindir}/
install -m 0755 -d $RPM_BUILD_ROOT/%{_sysconfdir}
install -D -m 0640 github.com/percona/percona-backup-mongodb/packaging/conf/pbm-agent-storage.conf $RPM_BUILD_ROOT/%{_sysconfdir}/pbm-agent-storage.conf
install -D -m 0640 github.com/percona/percona-backup-mongodb/packaging/conf/pbm-agent.conf $RPM_BUILD_ROOT/%{_sysconfdir}/pbm-agent.conf
%if 0%{?systemd}
  install -m 0755 -d $RPM_BUILD_ROOT/%{_unitdir}
  install -m 0644 github.com/percona/percona-backup-mongodb/packaging/conf/pbm-agent.service $RPM_BUILD_ROOT/%{_unitdir}/pbm-agent.service
%else
  install -m 0755 -d $RPM_BUILD_ROOT/etc/rc.d/init.d
  install -m 0750 github.com/percona/percona-backup-mongodb/packaging/rpm/pbm-agent.init $RPM_BUILD_ROOT/etc/rc.d/init.d/pbm-agent
%endif


%pre -n percona-backup-mongodb
/usr/bin/getent group pbm || /usr/sbin/groupadd -r pbm
/usr/bin/getent passwd pbm || /usr/sbin/useradd -r -s /sbin/nologin -g pbm pbm
if [ ! -d /run/pbm-agent ]; then
    install -m 0755 -d -opbm -gpbm /run/pbm-agent
fi
if [ ! -f /var/log/pbm-agent.log ]; then
    install -m 0640 -opbm -gpbm /dev/null /var/log/pbm-agent.log
fi


%post -n percona-backup-mongodb
%if 0%{?systemd}
  %systemd_post pbm-agent.service
  if [ $1 == 1 ]; then
      /usr/bin/systemctl enable pbm-agent >/dev/null 2>&1 || :
  fi
%else
  if [ $1 == 1 ]; then
      /sbin/chkconfig --add pbm-agent
  fi
%endif


%postun -n percona-backup-mongodb
case "$1" in
   0) # This is a yum remove.
      /usr/sbin/userdel pbm
      %if 0%{?systemd}
          %systemd_postun_with_restart pbm-agent.service
      %endif
   ;;
   1) # This is a yum upgrade.
      %if 0%{?systemd}
      %else
          /sbin/service pbm-agent condrestart >/dev/null 2>&1 || :
      %endif
   ;;
esac


%files -n percona-backup-mongodb
%{_bindir}/pbm-agent
%{_bindir}/pbm
%config(noreplace) %attr(0640,pbm,pbm) /%{_sysconfdir}/pbm-agent.conf
%config(noreplace) %attr(0640,pbm,pbm) /%{_sysconfdir}/pbm-agent-storage.conf
%if 0%{?systemd}
%{_unitdir}/pbm-agent.service
%else
/etc/rc.d/init.d/pbm-agent
%endif


%changelog
* Sun Dec 09 2018 Evgeniy Patlan <evgeniy.patlan@percona.com>
- First build
