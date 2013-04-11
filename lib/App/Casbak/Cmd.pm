package App::Casbak::Cmd;
use strict;
use warnings;
use Try::Tiny;
use Carp;
use Module::Runtime;

# Load a package for a casbak command (like 'ls', 'init', etc)
# Returns package name on success, false on nonexistent, and throws
# an exception if the package exists but fails to load.
sub LoadSubcommand {
	my ($class, $cmdName)= @_;
	
	# Convert underscore and hyphen to CamelCase
	my $submodule= join '', map { uc(substr($_,0,1)) . lc(substr($_,1)) } split /[-_]+/, $cmdName;
	# Is it a legal sub-package name?
	($submodule =~ /^[A-Za-z]+$/)
		or return 0;
	
	my $pkg= 'App::Casbak::Cmd::' . $submodule;
	my $err;
	try {
		Module::Runtime::require_module($pkg);
	}
	catch {
		$err= $_;
	};
	
	if (defined $err) {
		# Try to distinguish between module errors and nonexistent modules.
		my $commands= $class->FindAllCommands();
		return ''
			unless grep { $_ eq $pkg } @$commands;
		# looks like a bug in the package.
		die $err;
	}

	# Make sure the package implemented the required methods
	for my $mth (qw: ShortDescription applyArguments run getHelpPOD :) {
		die "Missing required method '$mth' in $pkg\n"
			if !$pkg->can($mth) or $pkg->can($mth) eq __PACKAGE__->can($mth);
	}
	return $pkg;
}

sub FindAllCommands {
	my ($class)= @_;
	my %pkgSet= ();
	# Search all include paths for packages named "App::Casbak::Cmd::*"
	for (@INC) {
		my $path= File::Spec->catdir($_, 'App', 'Casbak', 'Cmd');
		if (opendir(my $dh, $path)) {
			$pkgSet{"App::Casbak::Cmd::".substr($_,0,-3)}= 1
				for grep { $_ =~ /[.]pm$/ } readdir($dh);
		}
	}
	[ keys %pkgSet ]
}

sub wantVersion  { $_[0]{wantVersion} }
sub wantHelp     { $_[0]{wantHelp} }
sub verbosity    { $_[0]{verbosity} }
sub allowNoop    { $_[0]{allowNoop} }
sub casbakConfig { $_[0]{casbakConfig} }
sub backupDir    { $_[0]{casbakConfig}{backupDir} }

sub new {
	my $class= shift;
	my %p= @_ == 1? %{$_[0]} : @_;
	$class->_ctor(\%p);
}

sub _ctor {
	my ($class, $params)= @_;
	$params->{verbosity}   ||= 0;
	$params->{casbakConfig}||= {};
	$params->{casbakConfig}{backupDir} ||= '.';
	bless $params, $class;
}

sub applyArguments {
	my ($self, @args)= @_;
	
	# Iterate through options til the first non-option, which must be a sub-command name
	# (unless --help or --version was requested)
	require Getopt::Long;
	Getopt::Long::Configure(qw: no_ignore_case bundling require_order :);
	Getopt::Long::GetOptionsFromArray(\@args, $self->_baseGetoptConfig )
		or die "\n";

	# Now, figure out which subcommand to become
	if (@args) {
		my $cmd= shift @args;
		my $cmdClass= $self->LoadSubcommand($cmd)
			or die "No such command \"$cmd\"\n";
		
		$self= $cmdClass->_ctor($self);
		$self->can('applyArguments') eq \&applyArguments
			and die "Package '$cmdClass' did not implement 'applyArguments'";
		$self->applyArguments(@args);
	}
	else {
		die "No command specified\n"
			unless $self->wantVersion or $self->wantHelp or $self->allowNoop;
	}
}

sub run {
	croak "Subcommand required";
}

sub _baseGetoptConfig {
	my $self= shift;
	return
		'version|V'      => \$self->{wantVersion},
		'help|?'         => \$self->{wantHelp},
		'allow-noop'     => \$self->{allowNoop},
		'verbose|v'      => sub { ++$self->{verbosity} },
		'quiet|q'        => sub { --$self->{verbosity} },
		'casbak-dir|D=s' => \$self->{backupDir},
	;
}

sub ShortDescription {
	"(unimplemented)"
}

sub getHelpPOD {
	my $self= shift;
	
	# First, use dynamic module loading to find all the command classes
	my $commands= $self->FindAllCommands();

	# Try loading them all, but handle errors gracefully
	foreach my $pkg (@$commands) {
		try { Module::Runtime::require_module($pkg); };
	}

	# Now, format each command using POD notation, to make the body of the COMMANDS section.
	my $commandsPod=
		"=over 12\n"
		."\n";

	for my $pkg (sort @$commands) {
		my ($cmd)= ($pkg =~ /([^:]+)$/);

		$commandsPod .=
			"=item ".lc($cmd)."\n"
			."\n"
			.(try { $pkg->ShortDescription } catch { "(error loading module $pkg)" })."\n"
			."\n"
	}
	
	$commandsPod .=
		"\n"
		."=back\n";

	# Now read the source of this file
	my $source= do {
		open(my $f, '<', __FILE__)
			or die "Unable to read script (".__FILE__.") to extract help text: $!\n";
		local $/= undef;
		<$f>;
	};
	
	# And substitute the $commandsPod into the COMMANDS section
	($source =~ s/\n=head1 COMMANDS.*=head1/\n=head1 COMMANDS\n\n$commandsPod\n=head1/s )
		or warn "Internal error: failed to substitute command listing into help text.";
	
	return $source;
}

1;
__END__

=head1 NAME

casbak - backup tool using File::CAS

=head1 SYNOPSIS

  casbak [--casbak-dir=PATH] [-v|-q] COMMAND [--help]
  casbak --version
  casbak --help

Use "casbak --help" for a list of all installed commands.

=head1 COMMANDS

This list is dynamically generated from the installed modules by the casbak
script.  See --help

=head1 OPTIONS

The following options are available for all casbak commands:

=over 20

=item -D

=item --casbak-dir PATH

Path to the backup directory.  Defaults to "."

=item -v

=item --verbose

Enable output messages, and can be specified multiple times to enable
'INFO', then 'DEBUG', then 'TRACE'.  Verbose and quiet cancel eachother.

=item -q

=item --quiet

Disable output messages, and can be specified multiple times to disable
'NOTICE', then 'WARNING', then 'ERROR'.  Verbose and quiet cancel eachother.

=item -V

=item --version

Print the version of casbak (the utility) and File::CAS (the perl module) and exit.

=item -?

=item --help

Print this help, or help for the sub-command.

=back

=head1 EXAMPLES

  cd /path/to/backup

  # Backup the directories /usr/bin, /usr/local/bin, and /bin
  # storing each in the same-named location of the backup's hierarchy
  casbak import /usr/bin /usr/local/bin /bin
  
  # Backup the directory /tmp/new_bin as /bin in the backup's hierarchy
  casbak import /tmp/new_bin --as /bin
  
  # Restore the directory /etc from 3 days ago
  casbak export --date=3D /etc/ /etc/
  
  # List all modifications that have been performed on this backup
  casbak log
  
  # List files in /usr/local/share from 2 weeks ago without extracting them
  casbak ls -d 2W /usr/local/share
  
  # List files in /usr/local/share as of March 1st
  casbak ls --date 2012-03-01 /usr/local/share
  
  # Mount the snapshot of the filesystem from a year ago (using FUSE)
  casbak mount -d 1Y /mnt/temp

=head1 SECURITY

Some care should be taken regarding the permissions of the backup directory.
Casbak uses a plugin-heavy design.  If an attacker were able to modify the
configuration file in the backup directory, they could cause arbitrary perl
modules to be loaded.  If the attacker also had control of a directory in
perl's library path (or the environment variables of the backup script),
they would be able to execute arbitrary code as the user running casbak.
There may also be other exploits possible by modifying the backup config
file.  Ensure that only highly priveleged users have access to the backup
directory.

(Really, these precautions are common sense, as someone able to modify a
 backup, or access password files stored in the backup, or modify the
 environment of a backup script, or write to your perl module path would
 have a myriad of other ways to compromise your system.)

=cut