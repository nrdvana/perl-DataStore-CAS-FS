package App::Casbak::Cmd::Init;
use strict;
use warnings;
use Try::Tiny;

use parent 'App::Casbak::Cmd';

sub ShortDescription {
	"Initialize a new backup directory"
}

sub _ctor {
	my ($class, $params)= @_;

	$params->{casbakConfig} ||= {};
	$params->{casbakConfig}{cas} ||= {};
	$params->{casbakConfig}{cas}{store} ||= { CLASS => 'File::CAS::Store::Simple' };

	$class->SUPER::_ctor($params);
}

sub run {
	my $self= shift;
	App::Casbak->init($self->casbakConfig);
}

sub applyArguments {
	my ($self, @args)= @_;
	
	require Getopt::Long;
	Getopt::Long::Configure(qw: no_ignore_case bundling permute :);
	Getopt::Long::GetOptionsFromArray(\@args,
		$self->_baseGetoptConfig,
		'store|s=s'     => sub { $self->parseStore($_[1]) },
		'dirtype|d=s'   => sub { $self->parseDirtype($_[1]) },
		'digest=s'      => sub { $self->parseDigest($_[1]) },
		) or die "\n";

	for my $arg (@args) {
		($arg =~ /^([A-Za-z_][A-Za-z0-9_.]*)=(.*)/)
			or die "Invalid name=value pair: '$arg'\n";
		
		$self->apply($1, $2);
	}

	defined $self->casbakConfig->{cas}{store} and length $self->casbakConfig->{cas}{store}
		or die "Parameter 'cas.store' is required\n";
}

sub apply {
	my ($self, $path, $value)= @_;
	$path= [ split /\./, $path ] unless ref $path;
	
	my $node= $self->casbakConfig;
	for (my $i= 0; $i < $#$path; $i++) {
		my $field= $path->[$i];
		$node->{$field} ||= {};
		if (!ref $node->{$field}) {
			warn "using implied ".join('.', @$path[0..$i]).".CLASS = ".$node->{$field}."\n";
			$node->{$field}= { CLASS => $node->{$field} };
		}
		$node= $node->{$field};
	}
	warn "Multiple values specified for ".join('.', @$path).".  Using '$value'.\n"
		if defined $node->{$path->[-1]};
	$node->{$path->[-1]}= $value;
}

sub parseStore {
	my ($self, $spec)= @_;
	my %opts= (
		simple => 'File::CAS::Store::Simple',
	);
	
	my $pick= $opts{lc $spec}
		or die "Invalid store spec '$spec'\n";
	
	$self->apply('cas.store.CLASS' => $pick);
}

sub parseDirtype {
	my ($self, $spec)= @_;
	my %opts= (
		universal => 'File::CAS::Dir',
		minimal   => 'File::CAS::Dir::Minimal',
		unix      => 'File::CAS::Dir::Unix',
	);
	
	my $pick= $opts{lc $spec}
		or die "Invalid dirtype spec '$spec'\n";
	
	$self->apply('cas.scanner.dirClass' => $pick);
}

sub parseDigest {
	my ($self, $digest)= @_;
	$self->apply('cas.store.digest' => $digest);
}

sub getHelpPOD {
	open(my $f, '<', __FILE__)
		or die "Unable to read script (".__FILE__.") to extract help text: $!\n";
	local $/= undef;
	<$f>;
}

1;

__END__
=head1 NAME

casbak-init - initialize a casbak backup directory

=head1 SYNOPSIS

casbak [options] init [-s STORE_CLASS] [-d DIR_CLASS] [name=value ...]

STORE_CLASS is one of: 'Simple'

DIR_CLASS is one of: 'Universal', 'Minimal', 'Unix'

Each name=value pair is treated as an argument to the constructor of App::Casbak.
Use dotted notation to build a hierarchy, like "cas.store.digest=SHA-256".

See the documentation for App::Casbak, File::CAS, File::CAS::Store::*
and File::CAS::Scanner for all available constructor parameters.
Most of the important ones are given distinct options and described below.

=head1 OPTIONS

See "casbak --help" for general-purpose options.

=over 8

=item -D

=item --casbak-dir PATH

Specify an alternate directory in which to initialize the backup.
The default is the current directory.  (this is a general option,
but repeated here for emphasis)

=item -s

=item --store STORE_SPEC

This is a shorthand convenience for "cas.store.CLASS=".  You also do
not need to specify the full class name, and can use strings like
"Simple" to refer to File::CAS::Store::Simple.

Future popular stores might also have some sort of URL spec to both
indicate the type and connection parameters in one convenient string.

=item --digest ALGORITHM_NAME

This is a shorthand for "cas.store.digest=", and should apply to most
stores.  This controls which hash algorithm is used to hash the files.
ALGORITHM_NAME is passed directly to the Digest module constructor.
See "perldoc Digest" for the list available on your system.

=item -d

=item --dirtype CLASS

File::CAS can use a variety of different classes to encode directories.
This chooses the default for the store.  You can override it later if needed.

Note that the filesystem scanner determines what metadata is collected,
and this only determines which of that collected metadata can/will be
preserved in the backup.

This is a convenience for setting "cas.scanner.dirClass="

=over 12

=item Universal

use File::CAS::Dir, which encodes all metadata in JSON, which isn't terribly
efficient but can store anything you need to store.

=item Minimal

use File::CAS::Dir::Minimal, which encodes only a type, name, and value
(file digest, symlink target, device node, etc) and NO metadata like uid/gid,
permissions or even mtime.  This results in a very compact listing which
doesn't take much disk space.

Note that this is not suitable for incremental ('quick') backups because it
lacks a modify-timestamp.

=item Unix

use File::CAS::Dir::Unix, which stores all the standard Unix "stat()" values
in a relatively efficient (but portable) manner.  Timestamps are not limited
by 32-bit (which will become a major factor in the coming century).

=back

=back

=head1 SECURITY

See discussion in "casbak --help"

=cut

