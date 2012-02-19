package File::CAS::Dir;

use 5.006;
use strict;
use warnings;

=head1 NAME

File::CAS::Dir - Object representing a directory of file entries,
indexed by filename.

=head1 VERSION

Version 1.0000

=cut

our $VERSION= 1.0000;

=head1 SYNOPSIS

=head1 DESCRIPTION

This class handles the job of packing or unpacking a directory listing
to/from a stream of bytes.  Various subclasses exist, each supporting
a different selection of metadata fields.  For instance, the
File::CAS::Dir module itself only stores filename and content references
(and a few other things like symlink targets and device file nodes)

See the File::CAS::Dir::UnixStat if you want to store entire 'stat'
entries for each file.  Eventually there will also be a
File::CAS::Dir::UnixAttr if you want to store ACLs and Extended
Attributes, a Dir::Fat32 for fat32, and a Dir::Windows for ACL-based
windows permissions.

*Every* module should support all known filesystem entity types.
They only differ in which metadata they keep for that entry.

Note that you cannot have a File::CAS::Dir object until it has been
serialized and deserialized again.  All File::CAS::Dir objects are
full constructed and immutable.

To create a new File::CAS::Dir, you must first build a directory
listing in the format returned by File::CAS::DirScan, and then
serialize it using an appropriate Directory class's SerializeEntries()
method.

=head1 ATTRIBUTES

=head2 file (read-only, required)

The file this directory was deserialized from.

=head2 store (alias)

Alias for $dir->file->store

=head2 hash (alias)

Alias for $dir->file->hash

=head2 size (alias)

Alias for $dir->file->size

=head2 name (alias)

Alias for $dir->file->name

=cut

use Carp;
use Params::Validate ();

sub file     { $_[0]{file} }
sub store    { $_[0]{file}->store }
sub hash     { $_[0]{file}->hash }
sub size     { $_[0]{file}->size }

sub name     { $_[0]{file}->name }

=head1 FACTORY FUNCTIONS

=head2 $class->RegisterFormat( $format => $dirClass )

Registers a directory format for the File::CAS::Dir->new
factory behavior.  (i.e. File::CAS::Dir->new auto-detects the
format of a serialized directory, and creates an instance of
an appropriate class.

Typically the format is the same as the name of the $dirClass.
The only time they should be different is if you want to register
an alternate decoder for a known encoding.

=cut
our %_Formats= ( 'File::CAS::Dir' => 'File::CAS::Dir' );
sub RegisterFormat {
	my ($class, $format, $decoderClass)= @_;
	$decoderClass->isa($class)
		or croak "Must inherit from $class";
	$_Formats{$format}= $decoderClass;
}

=head2 $class->new( $file<File::CAS::File> )

This factory method reads the first few bytes of the File::CAS::File
data to determine which type of object to create.

That object might then read in the rest of the directory, or read
the entries on demand.

=cut
our $_MagicNumber= 'CAS_Dir ';
sub new {
	my ($class, $file)= @_;
	
	# first 8 bytes are "CAS_Dir "
	# Next 2 bytes are the length of the format in uppercase ascii hex (limiting format to 255 characters)
	# The byte after that is a space character.
	# There is a newline (\n) at the end of the format string which is not part of that count.
	$file->readall(my $buf, length $_MagicNumber) eq $_MagicNumber
		or croak "Bad magic number";
	my $formatLen= hex $file->readall($buf, 2);
	
	# once we get the name of the format, we can jump over to the constructor
	# for the appropriate class
	$file->readall(my $format, 1+$formatLen+1);
	substr($format, 1+$formatLen, 1) eq "\n"
		or croak "Invalid format encoding";
	$format= substr($format, 1, -1);
	defined $_Formats{$format}
		or croak "Invalid directory format '$format' (be sure to load relevant modules)";
	
	$_Formats{$format}->_ctor({file => $file});
}

=head1 METHODS

=head2 $class->SerializeEntries( \@entries, \%metadata )

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
my $_Encoder;
sub _Encoder { $_Encoder ||= JSON->new->utf8->canonical }

sub SerializeEntries {
	my ($class, $entryList, $metadata)= @_;
	require JSON;
	ref($metadata) eq 'HASH' or croak "Metadata must be a hashref"
		if $metadata;
	my $enc= _Encoder();
	my $json= $enc->encode($metadata || {});
	my $ret= "CAS_Dir 0E File::CAS::Dir\n"
		."{\"metadata\":$json,\n"
		." \"entries\":[ \n";
	$ret .= $enc->encode($_).",\n"
		for sort {$a->{name} cmp $b->{name}} @$entryList;
	substr($ret, -2)= "\n]}\n";
	$ret;
}

=head2 $class->_ctor( \%params )

Private-ish constructor.  Like "new" with no error checking, and requires a blessable hashref.

Only one parameter "file" is defined currently.

=cut
sub _ctor {
	my ($class, $params)= @_;
	require JSON;
	my $self= bless $params, $class;
	my $json= $self->file->slurp;
	my $data= _Encoder()->decode($json);
	$self->{_entries}= $data->{entries} or croak "Directory data is missing 'entries'";
	$self->{_metadata}= $data->{metadata} or croak "Directory data is missing 'metadata'";
	$self;
}

sub _entries { $_[0]{_entries} }

sub _entryHash {
	$_[0]{_entryHash} ||= { map { $_->{name} => $_ } @{$_[0]->_entries} };
}

sub find {
	$_[0]->_entryHash->{$_[1]};
}

1;