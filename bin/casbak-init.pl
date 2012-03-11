#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use App::Casbak;

my %init;

GetOptions(
	'version|V'          => sub { print $App::Casbak::VERSION."\n"; },
	'help|?'             => sub { pod2usage(-verbose => 2, -exitcode => 1) },
	'backup-dir|D=s'     => \$init{backupDir},
	'store|s=s'          => \&parseStore,
	'dirtype|d=s'        => \&parseDirtype,
) or pod2usage(2);

for my $arg (@ARGV) {
	($arg =~ /^([A-Za-z_][A-Za-z0-9_.]*)=(.*)/)
		or pod2usage("Invalid name=value pair: '$arg'\n");
	
	apply(\%init, [split /\./, $1], $2, 0 );
}

defined $init{cas}{store} and length $init{cas}{store}
	or pod2usage("Parameter 'cas.store' is required\n");

App::Casbak->init(\%init);
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

__END__
=head1 NAME

casbak-init - initialize a casbak backup directory

=head1 SYNOPSIS

casbak-init [options] -s STORE_CLASS [name=value [...]]

STORE_CLASS is one of: 'Simple'.  (more to come...)

Each name=value pair is treated as an argument to the constructor of App::Casbak.
See the documentation for App::Casbak, File::CAS, File::CAS::Store::*
and File::CAS::Scanner for all available constructor parameters.
Use dotted notation to build a hierarchy, like "cas.store.digest=sha256".

=head1 OPTIONS

=over 8

=item -D

=item --backup-dir DIR

Specify an alternate directory in which to initialize the backup.
The default is the current directory.

=item -s

=item --store STORE_SPEC

This is a shorthand convenience for "cas.store.CLASS=".  You also do
not need to specify the full class name, and can use strings like
"Simple" to refer to File::CAS::Store::Simple.

Future popular stores might also have some sort of URL spec to both
indicate the type and connection parameters in one convenient string.

=item -d

=item --dirtype CLASS

File::CAS can use a variety of different classes to encode directories.
This chooses the default for the store.  You can override it later if needed.

Note that the scanner determines what metadata is collected, and this only
determines which of that collected metadata will be preserved.

This is a convenience for setting "cas.scanner.dirClass="

=over 12

=item Universal

use File::CAS::Dir, which encodes all metadata in JSON, which isn't terribly
efficient but can store anything you need to store.

=item Minimal

use File::CAS::Dir::Minimal, which encodes only a type, name, and value
(file digest, symlink target, device node, etc) and NO metadata like uid/gid,
permissions, or even mtime. Note that this is not suitable for incremental
backups, but is very very compact.

=item Unix

use File::CAS::Dir::Unix, which stores all the standard Unix "stat()" values
in a relatively efficient manner.

=back

=back

=cut

