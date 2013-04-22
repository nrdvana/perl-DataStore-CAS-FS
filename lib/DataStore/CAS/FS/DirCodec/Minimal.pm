package DataStore::CAS::FS::DirCodec::Minimal;
use 5.008;
use strict;
use warnings;
use Carp;
use Try::Tiny;
require JSON;
require DataStore::CAS::FS::NonUnicode;
require DataStore::CAS::FS::Dir;

use parent 'DataStore::CAS::FS::DirCodec';

our $VERSION= 1.0000;

__PACKAGE__->register_format('minimal' => __PACKAGE__);
__PACKAGE__->register_format('' => __PACKAGE__);

=head1 NAME

DataStore::CAS::FS::DirCodec::Minimal - Directory representation with minimal metadata

=head1 SYNOPSIS

=head1 DESCRIPTION

This class packs a directory as a list of [type, hash, filename], which is
very efficient, but omits metadata that you often would want in a backup.

This is primarily intended for making small frequent backups inbetween more
thorough nightly backups.

=head1 METHODS

=head2 $class->encode( \@entries, \%metadata, \%flags )

Serialize the given entries into a scalar.

Serialize the bare minimum fields of each entry.  Each entry will have 3
pieces of data saved: I<type>, I<name>, and I<ref>.

The %metadata is encoded using JSON, which isn't very compact, but if
you really want a minimal encoding you shouldn't provide metadata anyway.

=cut

our %_TypeToCode= ( file => 'f', dir => 'd', symlink => 'l', chardev => 'c', blockdev => 'b', pipe => 'p', socket => 's' );
our %_CodeToType= map { $_TypeToCode{$_} => $_ } keys %_TypeToCode;
sub encode {
	my ($class, $entry_list, $metadata, $flags)= @_;
	my @entries= map {
		my ($type, $ref, $name)= ref $_ eq 'HASH'?
			( $_->{type}, $_->{ref}, $_->{name} )
			: ( $_->type, $_->ref, $_->name );
		my $code= $_TypeToCode{$type}
			or croak "Unknown directory entry type '$type' for entry $_";
		defined $name
			or croak "Missing name for entry $_";
		defined $ref or $ref= '';

		utf8::encode($ref) if utf8::is_utf8($ref);
		utf8::encode($name) if utf8::is_utf8($name);

		croak "Name too long: '$name'" if 255 < length $name;
		croak "Value too long: '$ref'" if 255 < length $ref;
		pack('CCA', length($name), length($ref), $code).$name."\0".$ref."\0"
	} @$entry_list;
	
	my $ret= "CAS_Dir 00 \n";
	if ($metadata and scalar keys %$metadata) {
		my $enc= JSON->new->utf8->canonical->convert_blessed;
		$ret .= $enc->encode($metadata);
	}
	$ret .= "\0";
	$ret .= join('', sort { substr($a,3) cmp substr($b,3) } @entries );
	$ret;
}

sub decode {
	my ($class, $params)= @_;
	$params->{format}= $class->_read_format($params)
		unless defined $params->{format};
	my $bytes= $params->{data};
	my $handle= $params->{handle};
	# This implementation just processes the file as a whole.
	# Read it in if we don't have it yet.
	my $header_len= $class->_calc_header_length($params->{format});
	if (defined $bytes) {
		substr($bytes, 0, $header_len)= '';
	}
	else {
		defined $handle or $handle= $params->{file}->open;
		seek($handle, $header_len, 0) or croak "seek: $!";
		local $/= undef;
		$bytes= <$handle>;
	}
	
	my $meta_end= index($bytes, "\0");
	$meta_end >= 0 or croak "Missing end of metadata";
	if ($meta_end > 0) {
		my $enc= JSON->new()->utf8->canonical->convert_blessed;
		DataStore::CAS::FS::NonUnicode->add_json_filter($enc);
		$params->{metadata}= $enc->decode(substr($bytes, 0, $meta_end));
	} else {
		$params->{metadata}= {};
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
	return DataStore::CAS::FS::Dir->new(
		file => $params->{file},
		format => 'minimal', # we encode with format string '', but this is what we want the user to see.
		entries => \@ents,
		metadata => $params->{metadata}
	);
}

package DataStore::CAS::FS::DirCodec::Minimal::Entry;
use strict;
use warnings;
use parent 'DataStore::CAS::FS::DirEnt';

sub type { $_CodeToType{$_[0][0]} }
sub name { $_[0][1] }
sub ref  { $_[0][2] }
sub as_hash {
	my $self= shift;
	return $self->[3] ||= {
		type => $self->type,
		name => $self->name,
		(defined $self->[2]? (ref => $self->[2]) : ())
	};
}

1;