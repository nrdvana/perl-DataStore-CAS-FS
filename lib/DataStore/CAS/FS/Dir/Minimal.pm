package DataStore::CAS::FS::Dir::Minimal;
use 5.008;
use strict;
use warnings;
use Carp;
use Try::Tiny;
require JSON;

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

Inherits from L<DataStore::CAS::FS::Dir>

=head1 METHODS

=head2 $class->SerializeEntries( \@entries, \%metadata, \%flags )

Serialize the given entries into a scalar.

Serialize the bare minimum fields of each entry.  Each entry will have 3
pieces of data saved: I<type>, I<name>, and one of I<hash>, I<path_ref>,
or I<device> as is appropriate for I<type>.

The metadata is encoded using JSON, which isn't very compact, but if you
really want a minimal encoding you shouldn't provide metadata anyway.

=cut

our %_TypeToCode= ( file => 'f', dir => 'd', symlink => 'l', chardev => 'c', blockdev => 'b', pipe => 'p', socket => 's' );
our %_CodeToType= map { $_TypeToCode{$_} => $_ } keys %_TypeToCode;
sub SerializeEntries {
	my ($class, $entry_list, $metadata, $flags)= @_;
	my @entries= map { ref $_ eq 'HASH'? DataStore::CAS::FS::Dir::Entry->new($_) : $_ } @$entry_list;
	
	my $ret= "CAS_Dir 07 Minimal\n";
	if ($metadata and scalar %$metadata) {
		my $enc= JSON->new->utf8->canonical;
		$ret .= $enc->encode($metadata)."\0";
	}
	else {
		$ret .= "\0";
	}
	for my $e (sort {$a->name cmp $b->name} @entries) {
		my $code= $_TypeToCode{$e->type}
			or croak "Unknown directory entry type: ".$e->type;

		my $ref= $e->ref;
		defined $ref or $ref= '';
		utf8::encode($ref) if utf8::is_utf8($ref);

		my $name= $e->name;
		utf8::encode($name) if utf8::is_utf8($name);

		croak "Name too long: '$name'" if 255 < length $name;
		croak "Value too long: '$ref'" if 255 < length $ref;
		$ret .= pack('CCA', length($name), length($ref), $code).$name."\0".$ref."\0";
	}
	
	$ret;
}

sub _deserialize {
	my ($self, $params)= @_;
	my $bytes= $params->{data};
	my $handle= $params->{handle};
	# This implementation just processes the file as a whole.
	# Read it in if we don't have it yet.
	my $header_len= $self->_calc_header_length($self->format);
	if (defined $bytes) {
		substr($bytes, 0, $header_len)= '';
	}
	else {
		defined $handle or $handle= $self->file->open;
		seek($handle, $header_len, 0) or croak "seek: $!";
		local $/= undef;
		$bytes= <$handle>;
	}
	
	my $meta_end= index($bytes, "\0");
	$meta_end >= 0 or croak "Missing end of metadata";
	if ($meta_end > 0) {
		my $enc= JSON->new->utf8->canonical;
		$self->{metadata}= $enc->decode(substr($bytes, 0, $meta_end));
	}

	my $pos= $meta_end+1;
	my @ents;
	while ($pos < length($bytes)) {
		my ($nameLen, $refLen, $code)= unpack('CCA', substr($bytes, $pos, 3));
		my $end= $pos + 3 + $nameLen + 1 + $refLen + 1;
		($end <= length($bytes))
			or croak "Unexpected end of file";
		my $name= substr($bytes, $pos+3, $nameLen);
		my $ref= substr($bytes, $pos+3+$nameLen+1, $refLen);
		$ref= undef unless length $ref;
		push @ents, bless [ $code, $name, $ref ], __PACKAGE__.'::Entry';
		$pos= $end;
	}
	$self->{_entries}= \@ents;
}

sub _entries { $_[0]{_entries} }

sub iterator {
	return DataStore::CAS::FS::Dir::EntryIter->new($_[0]->_entries);
}

sub _entry_name_map {
	$_[0]->{_entry_name_map} ||= { map { $_->[1] => $_ } @{$_[0]->_entries} };
}

=head2 $ent= $dir->get_entry($name)

Get a directory entry by name.

=cut
sub get_entry {
	$_[0]->_entry_name_map->{$_[1]};
}

package DataStore::CAS::FS::Dir::Minimal::Entry;
use strict;
use warnings;
use parent 'DataStore::CAS::FS::Dir::Entry';

sub type { $_CodeToType{$_[0][0]} }
sub name { $_[0][1] }
sub ref  { $_[0][2] }
sub as_hash { return $_[0][3] ||= { type => $_[0]->type, name => $_[0]->name, ref => $_[0]->ref } }

1;