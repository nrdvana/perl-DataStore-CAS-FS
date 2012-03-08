#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;

=head1 NAME

casbak - backup tool using File::CAS

=head1 SYNOPSIS

  casbak [--cas=PATH] COMMAND
  
See casbak COMMAND --help for details on each command

=head1 ARGUMENTS

=over 12

=item init

Initialize a backup directory

=item import

Import files into a backup

=item export

Export files from a backup back to the filesystem

=item log

View a log of all modifications performed on the backup directory

=item ls

List files in the backup

=item mount

Use FUSE to mount a snapshot from the backup as a filesystem

=back

=head1 EXAMPLES

  cd /path/to/backup

  # Backup the directories /usr/bin, /usr/local/bin, and /bin
  # storing each in the same-named location of the backup's hierarchy
  casbak import /usr/bin /usr/local/bin /bin
  
  # Backup the directory /tmp/new_bin as /bin in the backup's hierarchy
  casbak import --as /bin /tmp/new_bin
  
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
  
=cut

pod2usage(-verbose => 2);