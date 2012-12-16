#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long 2.24 qw(:config no_ignore_case bundling permute);
use Pod::Usage;
use App::Casbak;

my %casbak;
my @paths;
my %import= ( paths => \@paths );

sub add_path {
	my $arg= ''.shift;
	push @paths, { real => $arg, virt => $arg };
}
sub set_virtual_path {
	pod2usage("No preceeding path for '--as'") unless scalar @paths;
	$paths[-1]{virt}= $_[1];
}

GetOptions(
	App::Casbak::CmdlineOptions(\%casbak),
	'quick'   => \$import{quick},
	'<>'      => \&add_path,
	'as=s'    => \&set_virtual_path,
) or pod2usage(2);

scalar(@paths)
	or pod2usage("No paths specified");

App::Casbak->new(\%casbak)->importFile(\%import);
exit 0;

__END__

=head1 NAME

casbak-import - import files from real filesystem to virtual CAS filesystem

=head1 SYNOPSIS

  casbak-import [options] PATH [--as VIRTUAL_PATH] ...

=head1 OPTIONS

=over 20

=item --as VIRTUAL_PATH

Files are backed up from PATH (canonical, fully resolved, no symlinks)
in the real filesystem to a virtual path in the backup by the same
(canonical) name.

If you want the backup to contain the files in a different layout than
the real filesystem, you may specify "--as VIRTUAL_PATH" after a target.

Note that (at this time) VIRTUAL_PATH may not contain symlinks.  This support
will be added in the future.

=item --quick

Skip the checksum calculation for a file if its timestamp
and size have not changed from the previous run.  This
requires a directory encoding that preserves mtime, and for
a previous backup of those files to exist at the specified
virtual path.  If "quick" mode is not available, it will
calculate checksums as normal (and warn you if "verbose"
is enabled).

=back

See "casbak --help" for general-purpose options.

=head1 EXAMPLES

  # Backup the directories /usr/bin, /usr/local/bin, and /bin
  # storing each in the same-named location of the backup's hierarchy
  casbak-import /usr/bin /usr/local/bin /bin

  # Backup the directory /tmp/new_bin as /bin in the backup's hierarchy
  casbak-import /tmp/new_bin --as /bin

  # Backup several directories, but hide the partition boundary
  #  which shows up after casbak resolves the symlinks.
  # Example Symlinks:
  #   /var -> /mnt/var
  #   /usr -> /mnt/sys/usr
  casbak-import /bin /sbin /etc /usr --as /usr /var --as /var

=head1 SECURITY

See discussion in "casbak --help"

=cut
