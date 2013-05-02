package DataStore::CAS::FS::DirCodec::Universal;
use 5.0080001;
use strict;
use warnings;
use Carp;
use Try::Tiny;
use JSON 2.53 ();

use parent 'DataStore::CAS::FS::DirCodec';

require DataStore::CAS::FS::NonUnicode;
require DataStore::CAS::FS::DirEnt;

our $VERSION= 1.0000;

__PACKAGE__->register_format( universal => __PACKAGE__ );

# ABSTRACT: Codec for saving all arbitrary fields of a DirEnt

=head1 SYNOPSIS

  require DataStore::CAS::FS::DirCodec::Universal
  
  my %metadata= ( foo => 1, bar => 42 );
  my @entries= ( { name => 'file1', type => 'file', ref => 'SHA1DIGESTVALUE', mtime => '1736354736' } );
  
  my $digest_hash= DataStore::CAS::FS::DirCodec->put( $cas, 'universal', \@entries, \%metadata );
  my $dir= DataStore::CAS::FS::DirCodec->load( $cas->get($digest_hash) );
  
  print Dumper( $dir->get_entry('file1') );

=head1 DESCRIPTION

This codec can store any arbitrary metadata about a file.  It uses JSON for
its encoding, so other languages/platforms should be able to easily interface
with the files this codec writes ... except for Unicode caveats.

=head2 Unicode

JSON requires that all data be proper Unicode, and some filenames might be
a sequence of bytes which is not a valid Unicode string.  While the high-ascii
bytes of these filenames could be encoded as unicode code-points, this would
create an ambiguity with the names that actually were Unicode.  Instead, I
wrap values which are intended to be a string of octets in an instance of
L<DataStore::CAS::Dir::NonUnicode>, which gets written into JSON as

  C<{ "*NonUnicode*": $bytes_as_codepoints }>

Any attribute which contains bytes >= 0x80 and which does not have Perl's
unicode flag set will be encoded this way, so that it comes back as it went in.

However, since filenames are intended to be human-readable, they are decoded as
unicode strings when appropriate, even if they arrived as octets which just
happened to be valid UTF-8.

=head1 METHODS

=head2 encode

  my $serialized= $class->encode( \@entries, \%metadata )

Serialize the given entries into a scalar.

@entries is an array of DirEnt objects or hashrefs mimicing them.

%metadata is a hash of arbitrary metadata which you want saved along with the
directory.

This "Universal" DirCodec serializes the data as a short one-line header
followed by a string of JSON. JSON isn't the most efficient format around,
but it has wide cross-platform support, and can store any arbitrary DirEnt
attributes that you might have, and even structure within them.

The serialization contains newlines in a manner that should make it convenient
to write custom processing code to inspect the contents of the directory
without decoding the whole thing with a JSON library.

If you add anything to the metadata, try to keep the data consistent so that
two encodings of the same directory are identical.  Otherwise, (in say, a
backup utility) you will waste disk space storing multiple copies of the same
directory.

=cut

sub encode {
	my ($class, $entry_list, $metadata)= @_;
	ref($metadata) eq 'HASH' or croak "Metadata must be a hashref"
		if $metadata;

	my @entries= sort { $a->{name} cmp $b->{name} }
		map {
			my %entry= %{ref $_ eq 'HASH'? $_ : $_->as_hash};
			defined $entry{name} or croak "Can't serialize nameless directory entry: ".encode_json(\%entry);
			defined $entry{type} or croak "Can't serialize typeless directory entry: ".encode_json(\%entry);
			\%entry;
		} @$entry_list;

	my $enc= JSON->new->utf8->canonical->convert_blessed;
	my $json= $enc->encode($metadata || {});
	my $ret= "CAS_Dir 09 universal\n"
		."{\"metadata\":$json,\n"
		." \"entries\":[\n";
	for (@entries) {
		# If any of our fields are a byte string that is not valid unicode,
		# We wrap them with "NonUnicode" objects.
		#_preserve_octets({}) for values %$_;
		$ret .= $enc->encode($_).",\n"
	}

	# remove trailing comma
	substr($ret, -2)= "\n" if @entries;
	return $ret."]}";
}
sub _preserve_octets {
	my $r= ref $_;
	if (!$r) {
		$_= DataStore::CAS::FS::NonUnicode->new($_)
			if !utf8::is_utf8($_) && !utf8::decode($_);
	} else {
		croak "Recursion within DirEnt data" if $_[0]{refaddr($_)}++;
		if ($r eq 'HASH') { &_preserve_octets for values %$_ }
		elsif ($r eq 'ARRAY') { &_preserve_octets for @$_ }
	}
}

=head2 decode

  $dir= $class->decode( %params )

Reverses C<encode>, to create a Dir object.

See L<DataStore::CAS::FS::DirCodec> for details on %params.

=cut

sub decode {
	my ($class, $params)= @_;
	defined $params->{format} or $params->{format}= $class->_read_format($params);
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

	my $dec= JSON->new()->utf8->canonical->convert_blessed;
	DataStore::CAS::FS::NonUnicode->add_json_filter($dec, 1);
	my $data= $dec->decode($bytes);
	defined $data->{metadata} && ref($data->{metadata}) eq 'HASH'
		or croak "Directory data is missing 'metadata'";
	defined $data->{entries} && ref($data->{entries}) eq 'ARRAY'
		or croak "Directory data is missing 'entries'";
	my @entries;
	for my $ent (@{$data->{entries}}) {
		push @entries, DataStore::CAS::FS::DirEnt->new($ent);
	};
	return DataStore::CAS::FS::Dir->new(
		file => $params->{file},
		format => $params->{format},
		entries => \@entries,
		metadata => $data->{metadata}
	);
}

1;