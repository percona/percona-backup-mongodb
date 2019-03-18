%undefine _missing_build_ids_terminate_build
%global debug_package %{nil}
Name:  percona-backup-mongodb
Version: @@VERSION@@
Release: @@RELEASE@@%{?dist}
Summary: MongoDB backup tool

Group:  Applications/Databases
License: ASL 2.0
URL:  https://github.com/percona/percona-backup-mongodb
Source0: percona-backup-mongodb-%{version}.tar.gz

BuildRequires: golang make 

%description
MongoDB backup tool

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
make %{?_smp_mflags}
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


%files
%{_bindir}/pbmctl
%{_bindir}/pbm-agent
%{_bindir}/pbm-coordinator


%changelog
* Sun Dec 09 2018 Evgeniy Patlan <evgeniy.patlan@percona.com>
- First build
