package DataStore::CAS::FS::Dir::Minimal;
use 5.006;
use strict;
use warnings;
use Carp;
use Try::Tiny;

use parent 'DataStore::CAS::FS::Dir';

__PACKAGE__->RegisterFormat(Minimal => __PACKAGE__);

our $VERSION= 1.0000;

=head1 NAME

DataStore::CAS::FS::Dir::Minimal - Directory representation with minimal metadata

=head1 SYNOPSIS

=head1 DESCRIPTION

This class packs a directory as a list of [type, hash, filename],
which is very efficient, but omits metadata that you often would
want in a backup.

=head1 ATTRIBUTES

Inherits from L<DataStore::CAS::FS::Dir::Minimal>

=head1 FACTORY FUNCTIONS

=head1 METHODS

=head2 $class->SerializeEntries( \@entries, \%metadata )

Serialize the given entries (each a hashref) into the filehandle OUT_FH.
This serializes them in File::CAS::Dir format, which is very basic and
only records the entry type, filename (8-bit string), and string
representing the content (usually a checksum, for files and directories,
but the literal link target for symlinks, and device number for dev nodes)

The metadata hash is encoded as JSON, and written first.  It may have
information about the expected character set of the file entries, or any
other points of interest from the DirScan module that would be useful to
know later.

If you add anything to the metadata, bewate that it must be encoded in
a consistent manner, or future serializations of the same directory might
not come out to the same checksum.  (which would waste disk space, but
otherwise doesn't break anything)

=cut

our %_TypeToCode= ( file => 'f', dir => 'd', symlink => 'l', chardev => 'c', blockdev => 'b', pipe => 'p', socket => 's' );
our %_CodeToType= map { $_TypeToCode{$_} => $_ } keys %_TypeToCode;
our %_ValFieldForType= ( f => 'hash', d => 'hash', l => 'symlink', c => 'device', b => 'device', p => '', s => '' );
sub SerializeEntries {
	my ($class, $entryList, $metadata)= @_;
	
	my $ret= "CAS_Dir 07 Minimal\n";
	
	for my $e (sort {$a->{name} cmp $b->{name}} @$entryList) {
		my $code= $_TypeToCode{$e->{type}}
			or croak "Unknown directory entry type: $e->{type}";
		my $val= $e->{$_ValFieldForType{$code}};
		defined $val or $val= '';
		croak "Name too long: '$e->{name}'" if 255 < length $e->{name};
		croak "Value too long: '$val'" if 255 < length $val;
		$ret .= pack('CCA', length($e->{name}), length($val), $code).$e->{name}."\0".$val."\0";
	}
	
	$ret;
}

=head2 $class->_ctor( \%params )

Private-ish constructor.  Like "new" with no error checking, and requires a blessable hashref.

Defined parameters are "file" and "format".

=cut
sub _ctor {
	my ($class, $params)= @_;
	bless $params, $class;
}

sub _build__entries {
	my $self= shift;
	$self->file->seek($self->_headerLenForFormat($self->{format}))
		or croak "seek: $!";
	my (@e, $buf, $pos);
	while (!$self->file->eof) {
		$self->file->readall($buf, 3);
		my ($nameLen, $valLen, $code)= unpack('CCA', $buf);
		$self->file->readall($buf, $nameLen+$valLen+2);
		push @e, bless [ $code, substr($buf, 0, $nameLen), substr($buf, $nameLen+1, $valLen) ], __PACKAGE__.'::Entry';
	}
	$self->{file}->close; # we're most likely done with it
	\@e;
}

sub _entries {
	$_[0]{_entries} ||= $_[0]->_build__entries();
}

sub _entryHash {
	$_[0]{_entryHash} ||= { map { $_->[1] => $_ } @{$_[0]->_entries} };
}

=head2 $ent= $dir->getEntry($name)

Get a directory entry by name.

=cut
sub getEntry {
	$_[0]->_entryHash->{$_[1]};
}

package DataStore::CAS::FS::Dir::Minimal::Entry;
use strict;
use warnings;
use parent 'DataStore::CAS::FS::Dir::Entry';

sub type { $_CodeToType{$_[0][0]} }
sub name { $_[0][1] }
sub hash { ($_[0][0] eq 'f' || $_[0][0] eq 'd')? $_[0][2] : undef }
sub symlink { $_[0][0] eq 'l'? $_[0][2] : undef }
sub device { ($_[0][0] eq 'b' || $_[0][0] eq 'c')? $_[0][2] : undef }
sub as_hash { return $_[0][3] ||= { map { defined ($_[0]->$_)? ($_ => $_[0]->$_) : () } qw: type name hash symlink device : } }

1;