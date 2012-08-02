#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long 2.24 qw(:config no_ignore_case bundling permute);
use Pod::Usage;
use App::Casbak;

my %casbak;
my %init= ( cas => { store => { CLASS => 'File::CAS::Store::Simple' } } );

GetOptions(
	App::Casbak::CmdlineOptions(\%casbak),
	'store|s=s'     => \&parseStore,
	'dirtype|d=s'   => \&parseDirtype,
	'digest=s'      => \&parseDigest,
) or pod2usage(2);

for my $arg (@ARGV) {
	($arg =~ /^([A-Za-z_][A-Za-z0-9_.]*)=(.*)/)
		or pod2usage("Invalid name=value pair: '$arg'\n");
	
	apply(\%init, [split /\./, $1], $2, 0 );
}

defined $init{cas}{store} and length $init{cas}{store}
	or pod2usage("Parameter 'cas.store' is required\n");

App::Casbak->init({%casbak, %init});
exit 0;

sub apply {
	my ($hash, $path, $value, $i)= @_;
	my $field= $path->[$i];
	if ($i < $#$path) {
		$hash->{$field} ||= {};
		if (!ref $hash->{$field}) {
			warn "using implied ".join('.', @$path[0..$i]).".CLASS = $value\n";
			$hash->{$field}= { CLASS => $hash->{$field} };
		}
		apply($hash->{$field}, $path, $value, $i+1);
	} else {
		warn "Multiple values specified for ".join('.', @$path).".  Using '$value'.\n"
			if defined $hash->{$field};
		$hash->{$field}= $value;
	}
}

sub parseStore {
	my ($opt, $spec)= @_;
	if ($spec =~ /simple/i) {
		$init{cas}{store}{CLASS}= 'File::CAS::Store::Simple';
	} else {
		pod2usage("Invalid store spec '$spec'");
	}
}

sub parseDirtype {
	my ($opt, $spec)= @_;
	if ($spec =~ /universal/i) {
		$init{cas}{scanner}{dirClass}= 'File::CAS::Dir';
	} elsif ($spec =~ /minimal/i) {
		$init{cas}{scanner}{dirClass}= 'File::CAS::Dir::Minimal';
	} elsif ($spec =~ /unix/i) {
		$init{cas}{scanner}{dirClass}= 'File::CAS::Dir::Unix';
	} else {
		pod2usage("Invalid dirtype spec '$spec'");
	}
}

sub parseDigest {
	my ($opt, $digest)= @_;
	Digest->new($digest)
		or die "Digest algorithm '$digest' is not available on this system.\n";
	$init{cas}{store}{digest}= $digest;
}

__END__
=head1 NAME

casbak-init - initialize a casbak backup directory

=head1 SYNOPSIS

casbak-init [options] [-s STORE_CLASS] [-d DIR_CLASS] [name=value ...]

STORE_CLASS is one of: 'Simple'

DIR_CLASS is one of: 'Universal', 'Minimal', 'Unix'

Each name=value pair is treated as an argument to the constructor of App::Casbak.
Use dotted notation to build a hierarchy, like "cas.store.digest=sha256".

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

