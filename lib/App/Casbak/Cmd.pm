package App::Casbak::Cmd;
use Moo;
use Try::Tiny;
use Carp;
use Module::Runtime;
use App::Casbak;

has want_version  => ( is => 'rw' );
has want_help     => ( is => 'rw' );
has verbosity     => ( is => 'rw', default => sub { 0 } );
has allow_no_op   => ( is => 'rw' );

has casbak_args  => ( is => 'rw', default => sub { +{backup_dir => '.'} } );

sub backup_dir {
	my $args= $_[0]->casbak_args;
	$args->{backup_dir}= $_[1] if @_ > 1;
	$args->{backup_dir}
}

sub parse_argv {
	my ($class, $argv, $p)= @_;
	$p ||= { };
	require Getopt::Long;
	Getopt::Long::Configure(qw: no_ignore_case bundling require_order :);
	Getopt::Long::GetOptionsFromArray(
		$argv,
		'version|V'      => sub { $p->{want_version}= "$_[1]" },
		'help|?'         => sub { $p->{want_help}= "$_[1]" },
		'allow-noop'     => sub { $p->{allow_no_op}= "$_[1]" },
		'verbose|v'      => sub { $p->{verbosity}++ },
		'quiet|q'        => sub { $p->{verbosity}-- },
		'casbak-dir|D=s' => sub { $p->{casbak_args}{backup_dir}= "$_[1]" },
	) or die $class->syntax_error('');
	if (@$argv) {
		my $cmd_name= shift @$argv;
		my $cmd_class= $class->load_subcommand($cmd_name)
			or die $class->syntax_error("No such command \"$cmd_name\"");
		my $cmd_parse= $cmd_class->can('parse_argv');
		croak "parse_argv not implemented in $cmd_class"
			unless defined $cmd_parse && $cmd_parse ne \&parse_argv;
		return $cmd_class->parse_argv($argv, $p);
	}
	return ($class, $p);
}

our %_Commands;
our %_CommandByPackage;

sub register_command {
	my $class= shift;
	my %info= (@_ == 1 && ref $_[0] eq 'HASH')? %{$_[0]} : @_;
	defined $info{$_} or croak "$_ is required"
		for qw( command class description pod );
	$_Commands{$info{command}}= \%info;
	$_CommandByPackage{$info{class}}= \%info;
}

# Load a package for a casbak command (like 'ls', 'init', etc)
# Returns package name on success, false on nonexistent, and throws
# an exception if the package exists but fails to load.
sub load_subcommand {
	my ($class, $cmdName)= @_;
	
	# Do we already have it?
	return $_Commands{$cmdName}{class}
		if defined $_Commands{$cmdName};
	
	# Else guess the module name and try to load it
	
	# Convert underscore and hyphen to CamelCase
	my $submodule= join '', map { uc(substr($_,0,1)) . lc(substr($_,1)) } split /[-_]+/, $cmdName;
	# Convert dot to underscore
	$submodule =~ tr/./_/;
	# Is it a legal sub-package name?
	($submodule =~ /^[A-Za-z_]+$/)
		or return '';
	
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
		my $packages= $class->find_all_subcommands();
		return ''
			unless grep { $_ eq $pkg } @$packages;
		# looks like a bug in the package.
		die $err;
	}

	return defined $_Commands{$cmdName}? $_Commands{$cmdName}{class} : '';
}

sub find_all_subcommands {
	my $class= shift;
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

sub load_all_subcommands {
	my $class= shift;
	my $packages= $class->find_all_subcommands();
	for my $pkg (@$packages) {
		try {
			Module::Runtime::require_module($pkg);
		};
	}
}

sub BUILD {
}

sub run {
	my $self= shift;

	# They specified '--version'. Print it and exit.
	if ($self->want_version) {
		print App::Casbak->VersionMessage();
		return 'no-op';
	}

	# They specified '--help'.  Print the full POD and exit.
	if ($self->want_help) {
		require Pod::Usage;
		Pod::Usage::pod2usage(-verbose => 2, -input => $self->get_pod_source, -exitval => 'noexit');
		return 'no-op';
	}

	die $self->syntax_error("Sub-command required")
		unless $self->allow_no_op;
	'no-op';
}

sub get_pod_source {
	my $class= shift;
	$class= ref $class if ref $class;
	if ($class ne __PACKAGE__ && defined $_CommandByPackage{$class}) {
		my $pod= $_CommandByPackage{$class}{pod};
		$pod= $pod->()
			if ref $pod && ref $pod eq 'CODE';
		return $pod;
	}
	
	# First, use dynamic module loading to find all the command classes
	$class->load_all_subcommands();

	# Now, format each command using POD notation, to make the body of the COMMANDS section.
	my $commandsPod=
		"=over 12\n"
		."\n"
		.join('', map {
			my ($pkg, $cmd)= ($_, $_ =~ /([^:]*)$/);
			"=item $_->{command}\n"
			."\n"
			."$_->{description}\n"
			."\n"
		} sort { $a->{command} cmp $b->{command} } values %_Commands)
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
	
	# Return it as a filehandle
	open my $f, '<', \$source or die "open(STRING): $!";
	return $f;
}

sub syntax_error {
	my ($class, $msg)= @_;
	$msg= { message => $msg, pod_source => $class->get_pod_source }
		unless ref $msg;
	return App::Casbak::Cmd::SyntaxError($msg);
}

package App::Casbak::Cmd::SyntaxError;
use Moo;

has message    => ( is => 'rw', required => 1 );
has pod_source => ( is => 'rw', required => 1 );

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