package App::Casbak::Cmd::Export;
use strict;
use warnings;
use Try::Tiny;

use parent 'App::Casbak::Cmd';

sub ShortDescription {
	"Export files from backup to real filesystem"
}

1;

=pod

GetOptions(
	App::Casbak::CmdlineOptions(\%casbak),
	'merge' => \$export->{merge},
) or pod2usage(2);

scalar(@ARGV) == 2
	or pod2usage("Exactly one source and one dest path required");

App::Casbak->new(\%casbak)->export(\%export);
exit 0;

__END__

=head1 NAME

casbak-export - restore files from the backup to a real filesystem

=head1 SYNOPSIS

  casbak-export [options] VIRTUAL_PATH PATH
  casbak-export [options] --merge VIRTUAL_PATH PATH

=head1 OPTIONS

=over 20

=item --quick

Rely on the timestamp and size of files (in the real filesystem) instead
of hashing them, when determining whether they need replaced.

=item --check

Verify the files being restored from the CAS by re-hashing them to make
sure they haven't been corrupted.  The check is done *before* overwriting
the destination filename.

=back

See "casbak --help" for general-purpose options.

=head1 SECURITY

See discussion in "casbak --help"

=cut
