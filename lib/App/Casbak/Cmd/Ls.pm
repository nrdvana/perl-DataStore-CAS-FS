package App::Casbak::Cmd::Ls;
use strict;
use warnings;
use Try::Tiny;

use parent 'App::Casbak::Cmd';

sub ShortDescription {
	'List virtual directories within a backup'
}

sub lsConfig { $_[0]{lsConfig} }
sub paths { $_[0]{lsConfig}{paths} }
sub date { $_[0]{lsConfig}{date}= $_[1] if @_ > 1; $_[0]{lsConfig}{date}; }
sub hash { $_[0]{lsConfig}{hash}= $_[1] if @_ > 1; $_[0]{lsConfig}{hash}; }

sub _ctor {
	my ($class, $params)= @_;

	$params->{lsConfig} ||= {};
	$params->{lsConfig}{paths} ||= [];

	$class->SUPER::_ctor($params);
}

sub add_path {
	my ($self, $pathSpec)= @_;
	if ($pathSpec =~ m|^@([^/]+)(/.*)$|) {
		push @{$self->paths}, { date => $1, path => $2 };
	} elsif ($pathSpec =~ m|^#([^/]+)(/.*)$|) {
		push @{$self->paths}, { hash => $1, path => $2 };
	} else {
		push @{$self->paths}, { hash => $self->hash, date => $self->date, path => "$pathSpec" };
	}
}

sub applyArguments {
	my ($self, @args)= @_;
	
	require Getopt::Long;
	Getopt::Long::Configure(qw: no_ignore_case bundling permute :);
	Getopt::Long::GetOptionsFromArray(\@args,
		$self->_baseGetoptConfig,
		'date=s'      => sub { $self->date($_[1]); $self->hash(undef); },
		'root=s'      => sub { $self->hash($_[1]); $self->date(undef); },
		'long|l'      => \$self->lsConfig->{long},
		'a'           => \$self->lsConfig->{all},
		'directory|d' => \$self->lsConfig->{directory},
		'<>'          => sub { $self->add_path($_[0]) },
		) or die "\n";
	
}

sub run {
	my $self= shift;
	push @{$self->paths}, { path => '/' }
		unless @{$self->paths};
	App::Casbak->new($self->casbakConfig)->ls($self->lsConfig);
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

casbak ls - List files in a backup, roughly as the ls command would

=head1 SYNOPSIS

casbak ls [options] PATH [ [options] PATH ... ]

=head1 OPTIONS

See "casbak --help" for general-purpose options.

=over 20

=item --date DATESPEC

Instead of listing from the latest backup, list from a snapshot at or earlier
than DATESPEC.  This can be specified multiple times, and will affect paths
that follow it on the command line.

DATESPEC can be either an absolute date in YYYY-MM-DDTHH:mm:SS format (in the
current time zone unless it ends with Z), or a
unix epoch number, or a relative notation like used by rdiff-backup, where
#W is a number of weeks ago, #D is a number of days ago, #M is a number of
months ago, and #Y is a number of years ago.

Example:

  1Y            1 year ago
  5W            5 weeks ago
  13D           13 days ago
  123456        1970-01-02T10:17:36Z
  2012-01-01    2012-01-01T05:00:00Z (when in America/New_York)
  2012-01-01Z   2012-01-01T00:00:00Z

Note that you can find the timestamps of all backup operations with
"casbak log".

As a shorthand, you can specify the date for a specific file as

  "@DATESPEC/PATH"

=item --root HASH_PREFIX

The --date option picks a virtual root based on date and the backup log.
If you want, you can instead pick a root directory by its hash.
The format of HASH will depend on which digest algorithm is being used,
but for the default of sha256 you may specify "enough" of the leading
hex digits to refer to a distinct directory, rather than the full hash.
(like in git)

The selected root remains in effect for all paths that follow it, unless
changed with another '--root' option.

Note that you *can* specify a hash of a non-root directory, and this will
give you a chroot-like effect on any symlinks you encounter, which may
not be what you want.

As a shorthand, you can specify a root for a specific file as

  "#HASH_PREFIX/PATH"

but you might need to quote it so your shell doesn't turn it into a comment.

=item -l

=item --long

Print a long listing.  That is, with all known metadata for the directory
entries.  This is much like "ls -l" except the metadata fields depend on
what was recorded.

=item -a

casbak displays hidden files by default.  This option is a dummy so that
people in the habit of "ls -la" don't get an error message.

=item -d

=item --directory

list directories and symlinks as single entries, instead of listing their
contents.

=back
  
=head1 EXAMPLES

  # List files in /usr/local/share from 2 weeks ago without extracting them
  casbak ls --date 2W /usr/local/share
  
  # List files in /usr/local/share as of March 1st
  casbak ls --date 2012-03-01 /usr/local/share

  # Long listing, show hidden files
  casbak ls -la --date 2012-03-01 /home/$USER
  
=head1 SECURITY

See discussion in "casbak --help"

=cut

