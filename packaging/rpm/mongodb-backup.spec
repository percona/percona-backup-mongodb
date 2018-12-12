Name:  mongodb-backup
Version: @@VERSION@@
Release: @@RELEASE@@%{?dist}
Summary: MongoDB backup tool

Group:  Applications/Databases
License: ASL 2.0
URL:  https://github.com/percona/mongodb-backup
Source0: mongodb-backup-%{version}.tar.gz

BuildRequires: upx golang make 

%description
MongoDB backup tool

%prep
%setup -q -n mongodb-backup-%{version}


%build
cd ../
export PATH=/usr/local/go/bin:${PATH}
export GOROOT="/usr/local/go/"
export GOPATH=$(pwd)/
export PATH="/usr/local/go/bin:$PATH:$GOPATH"
export GOBINPATH="/usr/local/go/bin"
mkdir -p src/github.com/percona/
mv mongodb-backup-%{version} src/github.com/percona/mongodb-backup
ln -s src/github.com/percona/mongodb-backup mongodb-backup-%{version}
cd src/github.com/percona/mongodb-backup
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
cd src/github.com/percona/mongodb-backup
make install DEST_DIR=$RPM_BUILD_ROOT/%{_bindir}


%files
%{_bindir}/pmbctl
%{_bindir}/pmb-agent
%{_bindir}/pmb-coordinator


%changelog
* Sun Dec 09 2018 Evgeniy Patlan <evgeniy.patlan@percona.com>
- First build
