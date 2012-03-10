#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use App::Casbak;

my %casbak;
my %import;

GetOptions(
	'version|V' => sub { print $App::Casbak::VERSION."\n"; },
	'help|?'    => sub { pod2usage(-verbose => 2, -exitcode => 1) },
	'cas|C=s'   => \$casbak{backupDir},
	'as=s'      => \$import{as},
) or pod2usage(2);

scalar(@ARGV)
	or pod2usage("No paths specified");

if ($import{as}) {
	scalar(@ARGV) == 1
		or die "Only one path allowed when using --as\n";
	$import{paths}= [ [ $ARGV[0], $import{as} ] ];
} else {
	$import{paths}= [ @ARGV ];
}

App::Casbak->new(\%casbak)->import(\%import);
exit 0;

__END__

=head1 NAME

casbak-import - import files from real filesystem to virtual CAS filesystem

=head1 SYNOPSIS

  casbak-import [options] PATH [PATH...]
  casbak-import [options] --as VIRTUAL_PATH PATH

=head1 OPTIONS

=over 20

=item --as VIRTUAL_PATH

If the "--as" option is not used, then the specified file PATH will be used
for both real and virtual files, and any number of paths can be backed up in
one execution.  Note that if PATH is relative, it will first be resolved to
its most canonical form (removing symlinks) to determine an implied
VIRTUAL_PATH.

If the "--as" option is used, only one path can be backed up, and it will be
stored as the specified VIRTUAL_PATH.

Note that (at this time) VIRTUAL_PATH may not contain symlinks.  This support
will be added in the future.

=item --cas BACKUP_DIR

Path to the backup directory.  Defaults to "."

=item --quick | -q

Skip the checksum calculation for a file if its timestamp
and size have not changed from the previous run.  This
requires a directory encoding that preserves mtime.

=back

=head1 EXAMPLES

  # Backup the directories /usr/bin, /usr/local/bin, and /bin
  # storing each in the same-named location of the backup's hierarchy
  casbak-import /usr/bin /usr/local/bin /bin
  
  # Backup the directory /tmp/new_bin as /bin in the backup's hierarchy
  casbak-import --as /bin /tmp/new_bin
  
=cut
