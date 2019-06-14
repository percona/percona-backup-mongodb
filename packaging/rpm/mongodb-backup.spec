%undefine _missing_build_ids_terminate_build
%global debug_package %{nil}
%{!?with_systemd:%global systemd 0}
%{?el7:          %global systemd 1}
%{?el8:          %global systemd 1}


Name:  percona-backup-mongodb-agent
Version: @@VERSION@@
Release: @@RELEASE@@%{?dist}
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
MongoDB backup Agents are in charge of receiving commands from the coordinator and run them.

The agent must run locally (connected to 'localhost') on every MongoDB instance (mongos and config servers included) in order to collect information about the instance and forward it to the coordinator. With that information, the coordinator can determine the best agent to start a backup or restore, to start/stop the balancer, etc.

%package -n percona-backup-mongodb-coordinator
Summary: MongoDB backup coordinator
Requires(pre): /usr/sbin/useradd, /usr/bin/getent
Requires(postun): /usr/sbin/userdel
%if 0%{?systemd}
BuildRequires:  systemd git
BuildRequires:  pkgconfig(systemd)
Requires(post):   systemd
Requires(preun):  systemd
Requires(postun): systemd
%else
Requires(post):   /sbin/chkconfig
Requires(preun):  /sbin/chkconfig
Requires(preun):  /sbin/service
%endif

%description -n percona-backup-mongodb-coordinator
The MongoDB backup coordinator is a daemon that handles communication with backup agents and the backup control program

%package -n percona-backup-mongodb-pbmctl
Summary: MongoDB backup command line utility to send commands to the coordinator

%description -n percona-backup-mongodb-pbmctl
MongoDB backup command line utility to send commands to the coordinator

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
cd src/github.com/percona/percona-backup-mongodb
make %{?_smp_mflags} build-all
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
cd src/github.com/percona/percona-backup-mongodb
make install DEST_DIR=$RPM_BUILD_ROOT/%{_bindir}
install -m 0755 -d $RPM_BUILD_ROOT/%{_sysconfdir}
install -D -m 0640 packaging/conf/pbm-agent-storages.conf $RPM_BUILD_ROOT/%{_sysconfdir}/pbm-agent-storages.conf
install -D -m 0640 packaging/conf/pbm-agent.conf $RPM_BUILD_ROOT/%{_sysconfdir}/pbm-agent.conf
install -D -m 0640 packaging/conf/pbm-coordinator.conf $RPM_BUILD_ROOT/%{_sysconfdir}/pbm-coordinator.conf
%if 0%{?systemd}
  install -m 0755 -d $RPM_BUILD_ROOT/%{_unitdir}
  install -m 0644 packaging/conf/pbm-coordinator.service $RPM_BUILD_ROOT/%{_unitdir}/pbm-coordinator.service
  install -m 0644 packaging/conf/pbm-agent.service $RPM_BUILD_ROOT/%{_unitdir}/pbm-agent.service
%else
  install -m 0755 -d $RPM_BUILD_ROOT/etc/rc.d/init.d
  install -m 0750 packaging/rpm/pbm-agent.init $RPM_BUILD_ROOT/etc/rc.d/init.d/pbm-agent
  install -m 0750 packaging/rpm/pbm-coordinator.init $RPM_BUILD_ROOT/etc/rc.d/init.d/pbm-coordinator
%endif


%pre -n percona-backup-mongodb-agent
/usr/bin/getent group pbm || /usr/sbin/groupadd -r pbm
/usr/bin/getent passwd pbm || /usr/sbin/useradd -r -s /sbin/nologin -g pbm pbm
if [ ! -d /run/pbm-agent ]; then
    install -m 0640 -d -opbm -gpbm /run/pbm-agent
fi
if [ ! -f /var/log/pbm-agent.log ]; then
    install -m 0640 -opbm -gpbm /dev/null /var/log/pbm-agent.log
fi


%post -n percona-backup-mongodb-agent
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

%post -n percona-backup-mongodb-coordinator
%if 0%{?systemd}
  %systemd_post pbm-coordinator.service
  if [ $1 == 1 ]; then
      /usr/bin/systemctl enable pbm-coordinator >/dev/null 2>&1 || :
  fi
%else
  if [ $1 == 1 ]; then
      /sbin/chkconfig --add pbm-coordinator
  fi
%endif

%postun -n percona-backup-mongodb-agent
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


%pre -n percona-backup-mongodb-coordinator
/usr/bin/getent group pbm || /usr/sbin/groupadd -r pbm
/usr/bin/getent passwd pbm || /usr/sbin/useradd -r -s /sbin/nologin -g pbm pbm
if [ ! -d /var/lib/pbm-coordinator ]; then
    install -m 0640 -d -opbm -gpbm /var/lib/pbm-coordinator
fi
if [ ! -f /var/log/pbm-coordinator.log ]; then
    install -m 0640 -opbm -gpbm /dev/null /var/log/pbm-coordinator.log
fi

%postun -n percona-backup-mongodb-coordinator
case "$1" in
   0) # This is a yum remove.
      /usr/sbin/userdel pbm
      %if 0%{?systemd}
          %systemd_postun_with_restart pbm-coordinator.service
      %endif
   ;;
   1) # This is a yum upgrade.
      %if 0%{?systemd}
      %else
          /sbin/service pbm-coordinator condrestart >/dev/null 2>&1 || :
      %endif
   ;;
esac

%preun -n percona-backup-mongodb-agent
%if 0%{?systemd}
  %systemd_preun pbm-agent.service
%else
  if [ "$1" = 0 ]; then
    /sbin/service pbm-agent stop >/dev/null 2>&1 || :
    /sbin/chkconfig --del pbm-agent
  fi
%endif

%preun -n percona-backup-mongodb-coordinator
%if 0%{?systemd}
  %systemd_preun pbm-coordinator.service
%else
  if [ "$1" = 0 ]; then
    /sbin/service pbm-coordinator stop >/dev/null 2>&1 || :
    /sbin/chkconfig --del pbm-coordinator
  fi
%endif


%files -n percona-backup-mongodb-agent
%{_bindir}/pbm-agent
%config(noreplace) %attr(0640,pbm,pbm) /%{_sysconfdir}/pbm-agent.conf
%config(noreplace) %attr(0640,pbm,pbm) /%{_sysconfdir}/pbm-agent-storages.conf
%if 0%{?systemd}
%{_unitdir}/pbm-agent.service
%else
/etc/rc.d/init.d/pbm-agent
%endif


%files -n percona-backup-mongodb-coordinator
%{_bindir}/pbm-coordinator
%config(noreplace) %attr(0640,pbm,pbm) /%{_sysconfdir}/pbm-coordinator.conf
%if 0%{?systemd}
%{_unitdir}/pbm-coordinator.service
%else
/etc/rc.d/init.d/pbm-coordinator
%endif


%files -n percona-backup-mongodb-pbmctl
%{_bindir}/pbmctl


%changelog
* Sun Dec 09 2018 Evgeniy Patlan <evgeniy.patlan@percona.com>
- First build
