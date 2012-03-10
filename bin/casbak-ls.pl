#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use App::Casbak;

my %casbak;
my %ls;

GetOptions(
	'version|V' => sub { print $App::Casbak::VERSION."\n"; },
	'help|?'    => sub { pod2usage(-verbose => 2, -exitcode => 1) },
	'cas|C'     => \$casbak{backupDir},
	'date|d'    => \$ls{date},
	'long|l'    => \$ls{long},
	'a'         => \$ls{hidden},
) or pod2usage(2);

$ls{paths}= [ @ARGV ];

App::Casbak->new(\%casbak)->ls(\%ls);
exit 0;


__END__
=head1 NAME

casbak-ls - List files in a backup, roughly as the ls command would

=head1 SYNOPSIS

casbak-ls [options] PATH

=head1 OPTIONS

=over 20

=item --cas BACKUP_PATH

Use the backup at the specified path rather than the current directory.

=item --date | -d DATESPEC

Instead of listing from the latest backup, list from a snapshot at or earlier
than DATESPEC.

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

=item -l --long

Print a long listing.  That is, with all known metadata for the directory
entries.  This is much like "ls -l" except the metadata fields are slightly
different.

=item -a

casbak displays hidden files by default.  This option is a dummy so that
people in the habit of "ls -la" don't get an error message.

=back
  
=head1 EXAMPLES

  # List files in /usr/local/share from 2 weeks ago without extracting them
  casbak ls -d 2W /usr/local/share
  
  # List files in /usr/local/share as of March 1st
  casbak ls --date 2012-03-01 /usr/local/share

  # Long listing, show hidden files
  casbak ls -la --date 2012-03-01 /home/$USER
  
=cut
