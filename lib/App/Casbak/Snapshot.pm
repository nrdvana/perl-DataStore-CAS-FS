package App::Casbak::Snapshot;

use strict;
use warnings;
use Carp;

use parent 'File::CAS::Dir';

File::CAS::Dir->RegisterFormat(__PACKAGE__, __PACKAGE__);

sub timestamp { $_[0]->{_metadata}{timestamp} }
sub comment   { $_[0]->{_metadata}{comment} }

sub SerializeEntries {
	my ($class, $entryList, $metadata)= @_;
	# We use the same encoding as the default dir class, but swap the magic number.
	my $ret= $class->SUPER::SerializeEntries($entryList, $metadata);
	($ret =~ s/^CAS_Dir 0E File::CAS::Dir\n/CAS_Dir 15 App::Casbak::Snapshot\n/)
		or croak "Unexpected directory encoding in parent class";
	$ret;
}

sub _ctor {
	my $class= shift;
	my $self= $class->SUPER::_ctor(@_);
	$self->getRootEntry
		or die "Snapshot is missing root entry\n";
	# TODO: For windows, might make sense to have entries for C:, D:, etc
	$self->{_metadata}{timestamp}
		or die "Snapshot missing timestamp\n";
}

sub rootEntry {
	$_[0]->getEntry('')
}

1;