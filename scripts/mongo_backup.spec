%define		git_repo	MongoToolsAndSnippets
%define		git_url		https://github.com/timvaillancourt/%{git_repo}.git
%define		git_branch	consistent_backupv1

Name:		mongo_backup
Version:	0.0.1
Release:	1%{?dist}
Summary:	MongoDB Consistent Backup Tool

Group:		Software/Databases
License:	TBD
URL:		https://github.com/percona/MongoToolsAndSnippets

BuildRequires:	python	
Requires:	python

Prefix:		/usr

%description
Creates cluster-consistent point-in-time backups of MongoDB via wrapping 'mongodump'.
Backups are remotely-pulled and outputted onto the host running the tool


%prep
[ -d %{git_repo} ] && rm -rf %{git_repo}
git clone -b %{git_branch} %{git_url} %{git_repo}


%build
make -C %{git_repo}/rdba/mongo_backup


%install
make -C %{git_repo}/rdba/mongo_backup PREFIX=%{prefix} DESTDIR=%{buildroot} install


%files
%{prefix}/bin/mongo_backup


%changelog

