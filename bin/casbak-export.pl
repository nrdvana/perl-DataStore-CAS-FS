#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;

=head1 NAME

casbak-export - restore files from the backup to a real filesystem

=head1 SYNOPSIS

  casbak-export [options] VIRTUAL_PATH PATH
  casbak-export [options] --merge VIRTUAL_PATH PATH

=head1 OPTIONS

=over 20

=item --cas BACKUP_DIR

Path to the backup directory.  Defaults to "."

=item --check | -c

Check the checksum of each file instead of assuming based on
timestamp, and also check each restored file.

=back

=cut

pod2usage(1);
