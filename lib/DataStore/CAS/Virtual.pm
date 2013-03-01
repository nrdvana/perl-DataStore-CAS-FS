package DataStore::CAS::Virtual;
use 5.008;
use strict;
use warnings;
use Carp;
use Try::Tiny;
use Digest;

use parent 'DataStore::CAS';

our @_ctor_params= qw: entries :;
sub _ctor_params { @_ctor_params, $_[0]->SUPER::_ctor_params; }

sub _ctor {
	my ($class, $params)= @_;
	my %p= map { $_ => delete $params->{$_} } @_ctor_params;
	$params->{digest} ||= 'SHA-1';
	my $self= $class->SUPER::_ctor($params);
	$self->{entries}= $p{entries} || {};
	return $self;
}

sub entries { $_[0]{entries} ||= {} }

sub get {
	my ($self, $hash)= @_;
	return undef unless defined $self->entries->{$hash};
	return bless { store => $self, hash => $hash, size => length($self->entries->{$hash}) }, 'DataStore::CAS::File';
}

sub put_scalar {
	my ($self, $data, $flags)= @_;

	my $hash= ($flags and defined $flags->{known_hash})? $flags->{known_hash}
		: Digest->new($self->digest)->add($data)->hexdigest;

	$self->entries->{$hash}= $data
		unless $flags and $flags->{dry_run};

	$hash;
}

sub new_write_handle {
	my ($self, $flags)= @_;
	my $data= {
		buffer  => '',
		flags   => $flags
	};
	return DataStore::CAS::FileCreatorHandle->new($self, $data);
}

sub _handle_write {
	my ($self, $handle, $buffer, $count, $offset)= @_;
	my $data= $handle->_data;
	utf8::encode($buffer) if utf8::is_utf8($buffer);
	$offset ||= 0;
	$count ||= length($buffer)-$offset;
	$data->{buffer} .= substr($buffer, $offset, $count);
	return $count;
}

sub _handle_seek {
	croak "Seek unsupported (for now)"
}

sub _handle_tell {
	my ($self, $handle)= @_;
	return length($handle->_data->{buffer});
}

sub commit_write_handle {
	my ($self, $handle)= @_;
	return $self->put_scalar($handle->_data->{buffer}, $handle->_data->{flags});
}

sub open_file {
	my ($self, $file, $flags)= @_;
	open(my $fh, '<', \$self->entries->{$file->hash})
		or die "open: $!";
	return $fh;
}

1;