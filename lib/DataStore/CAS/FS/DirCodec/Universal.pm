package DataStore::CAS::FS::DirCodec::Universal;
use 5.008;
use strict;
use warnings;
use parent 'DataStore::CAS::FS::DirCodec';
use Carp;
use Try::Tiny;
require JSON;
require DataStore::CAS::FS::NonUnicode;

our $VERSION= 1.0000;

__PACKAGE__->register_format( universal => __PACKAGE__ );

=head1 NAME

DataStore::CAS::FS::DirCodec::Universal - Codec for saving all arbitrary
metadata about a file.

=head1 SYNOPSIS

  require DataStore::CAS::FS::DirCodec::Universal
  
  my %metadata= ( foo => 1, bar => 42 );
  my @entries= ( { name => 'file1', type => 'file', ref => 'SHA1DIGESTVALUE', mtime => '1736354736' } );
  
  my $digest_hash= DataStore::CAS::FS::DirCodec->store( $cas, 'universal', \@entries, \%metadata );
  my $dir= DataStore::CAS::FS::DirCodec->load( $cas->get($digest_hash) );
  
  print Dumper( $dir->get_entry('file1') );

=head1 DESCRIPTION

This codec can store any arbitrary metadata about a file.  It uses JSON for
its encoding, so other languages/platforms should be able to easily interface
with the files this codec writes.  ... except for Unicode.

=head2 Unicode

JSON requires that all data be encoded in Unicode.  Some filenames might be
a sequence of bytes which are not valid Unicode strings.  While the high-ascii
bytes of these filenames could be encoded as unicode code-points, this would
create an ambiguity with the names that actually were Unicode.  Instead, I
wrap values which are intended to be a string of octets in an instance of
L<DataStore::CAS::Dir::NonUnicode>, which gets written into JSON as

  C<{ "*NonUnicode*": $bytes_as_codepoints }>

The 'ref' attribute of DirEnt is also encoded this way.  Any attribute
has the potential to be encoded this way depending on whether the Scanner
(which read it from the filesystem) decided to wrap it in a NonUnicode object.

=head1 METHODS

=head2 $class->encode( \@entries, \%metadata )

Serialize the given entries into a scalar.

This serializes them in File::CAS::Dir format, which uses JSON and isn't
too efficient.  The benefit is that it will store *any* keys you add to the
directory entry, and restore them to the same Perl data structure you had
before.  (excluding blessings and ties and etc)

If you add anything to the metadata, beware that it must be encoded in
a consistent manner, or future serializations of the same directory might
not come out to the same checksum.  (which would waste disk space, but
otherwise doesn't break anything)

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
			# Convert all name strings down to plain bytes, for our sort
			# (they should be already)
			utf8::encode($entry{name})
				if !ref $entry{name} and utf8::is_utf8($entry{name});
			\%entry;
		} @$entry_list;

	my $enc= JSON->new->utf8->canonical->convert_blessed;
	my $json= $enc->encode($metadata || {});
	my $ret= "CAS_Dir 09 universal\n"
		."{\"metadata\":$json,\n"
		." \"entries\":[\n";
	for (@entries) {
		# The name field is plain bytes, and *might* not be valid UTF-8.
		# JSON module will force it to be UTF-8 (or encode the high-ascii
		# bytes as codepoints, which would be confusing later)
		# We test for that case, and wrap it in a NonUnicode which gets
		# specially serialized into JSON.
		if (!utf8::is_utf8($_->{name}) and !utf8::decode($_->{name})) {
			$_->{name}= DataStore::CAS::FS::NonUnicode->new($_->{name});
		}
		# ref should also be treated as octets.
		if (!utf8::is_utf8($_->{ref}) and !utf8::decode($_->{ref})) {
			$_->{ref}= DataStore::CAS::FS::NonUnicode->new($_->{ref});
		}
		# Any other field with high bytes without the unicode flag should be
		# wrapped by the thing that writes it.
		$ret .= $enc->encode($_).",\n"
	}

	# remove trailing comma
	substr($ret, -2)= "\n" if @entries;
	return $ret."]}\n";
}

=head2 $class->decode( \%params )

Params 'file' and 'format' have already been initialized, and if the
parameters 'handle' and 'data' were specified they are forwarded to this
method.

"data" is the complete data of the file, and if present should eliminate the
need to open the file.

"handle" is an open file handle to the data of the file, and should be used
if provided.

If neither is given, this calls file->open to get a handle to work with.

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

	my $enc= JSON->new()->utf8->canonical->convert_blessed;
	DataStore::CAS::FS::NonUnicode->add_json_filter($enc);
	my $data= $enc->decode($bytes);
	defined $data->{metadata} && ref($data->{metadata}) eq 'HASH'
		or croak "Directory data is missing 'metadata'";
	defined $data->{entries} && ref($data->{entries}) eq 'ARRAY'
		or croak "Directory data is missing 'entries'";
	my @entries;
	for my $ent (@{$data->{entries}}) {
		# While name and ref are probably logically unicode, we want them
		#  kept as octets for compatibility reasons.
		utf8::encode($ent->{name})
			if !ref $ent->{name} and utf8::is_utf8($ent->{name});
		utf8::encode($ent->{ref})
			if !ref $ent->{ref}  and utf8::is_utf8($ent->{ref});

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