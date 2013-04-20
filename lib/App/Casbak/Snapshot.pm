package App::Casbak::Snapshot;
use Moo;
use Carp;

has cas        => ( is => 'ro', required => 1 );
has root_entry => ( is => 'ro', required => 1 );
has metadata   => ( is => 'ro', required => 1 );

sub timestamp { $_[0]->metadata->{timestamp} }
sub comment   { $_[0]->metadata->{comment} }

=head1 METHODS

=head2 get_fs()

Returns a new DataStore::CAS::FS object to view the snapshot.

=cut

sub new_fs {
	DataStore::CAS::FS->new( store => $self->cas, root_entry => $self->root_entry );
}

1;