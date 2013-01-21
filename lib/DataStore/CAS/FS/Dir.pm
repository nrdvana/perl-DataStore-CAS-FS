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

Alias for file->store

=head2 hash (alias)

Alias for file->hash

=head2 size (alias)

Alias for file->size

=head2 name (alias)

Alias for file->name

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
		or croak "$decoderClass must inherit from $class";
	$_Formats{$format}= $decoderClass;
}

=head2 $class->new( $file<File::CAS::File> )

This factory method reads the first few bytes of the File::CAS::File
data to determine which type of object to create.

That object might then read in the rest of the directory, or read
the entries on demand.

=cut
our $_MagicNumber= 'CAS_Dir ';
sub _headerLenForFormat {
	my ($class, $format)= @_;
	return length($_MagicNumber)+2+1+length($format)+1;
}
sub _readFormat {
	my ($class, $file)= @_;
	
	$file->seek(0,0);
	
	# first 8 bytes are "CAS_Dir "
	# Next 2 bytes are the length of the format in uppercase ascii hex (limiting format to 255 characters)
	# The byte after that is a space character.
	# There is a newline (\n) at the end of the format string which is not part of that count.
	$file->readall(my $buf, length $_MagicNumber) eq $_MagicNumber
		or croak "Bad magic number in directory ".$file->hash;
	my $formatLen= hex $file->readall($buf, 2);
	
	$file->readall($buf, 1+$formatLen+1);
	substr($buf, 0, 1) eq ' ' && substr($buf, -1, 1) eq "\n"
		or croak "Invalid directory encoding in ".$file->hash;
	return substr($buf, 1, -1);
}

sub new {
	my ($class, $file)= @_;
	# as a convenience, you may pass a null file, which creates a null directory.
	return undef unless $file;
	# Once we get the name of the format, we can jump over to the constructor
	# for the appropriate class
	my $format= $class->_readFormat($file);
	defined $_Formats{$format}
		or croak "Unknown directory format '$format' in ".$file->hash."\n(be sure to load relevant modules)";
	
	$_Formats{$format}->_ctor({ file => $file, format => $format });
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
		." \"entries\":[\n";
	$ret .= $enc->encode(ref $_ eq 'HASH'? $_ : $_->asHash).",\n"
		for sort {(ref $a eq 'HASH'? $a->{name} : $a->name) cmp (ref $b eq 'HASH'? $b->{name} : $b->name)} @$entryList;
	substr($ret, -2)= "\n]}\n";
	$ret;
}

=head2 $class->_ctor( \%params )

Private-ish constructor.  Like "new" with no error checking, and requires a blessable hashref.

Required parameters are "file" and "format".  Format must be the type encoded in the file, or
deserialization will fail.

=cut
sub _ctor {
	my ($class, $params)= @_;
	require JSON;
	my $self= bless $params, $class;
	
	$self->file->seek($class->_headerLenForFormat($params->{format}));
	my $json= $self->file->slurp;
	my $data= _Encoder()->decode($json);
	$self->{_entries}= $data->{entries} or croak "Directory data is missing 'entries'";
	$_= File::CAS::Dir::Entry->new($_) for @{$self->{_entries}};
	$self->{_metadata}= $data->{metadata} or croak "Directory data is missing 'metadata'";
	$self;
}

=head2 $dir->find(@path)

Find a dir entry for the specified path.  If the path does not exist, returns undef.

Throws exceptions if it encounters invalid directories, or has read errors.
All other failures cause an early "return undef".

=cut
sub find {
	my ($self, $name, @path)= @_;
	if (@path) {
		my $subdir= $self->subdir($name)
			or return undef;
		return $subdir->find(@path);
	}
	$self->getEntry($name);
}

sub getEntries { @{$_[0]{_entries}} }

=head2 $ent= $dir->getEntry($name)

Get a directory entry by name.

=cut
sub _entryHash {
	$_[0]{_entryHash} ||= { map { $_->name => $_ } $_[0]->getEntries };
}
sub getEntry {
	return $_[0]->_entryHash->{$_[1]};
}

=head2 $dir2= $dir->subdir($name)

Like getEntry, this finds an entry, but it also expands that entry into
a File::CAS::Dir object.

=cut
sub subdir {
	my ($self, $name)= @_;
	my $entry= $self->getEntry($name)
		or return undef;
	($entry->type eq 'dir' && defined $entry->hash)
		or return undef;
	return File::CAS::Dir->new($self->file->store->get($entry->hash));
}

package File::CAS::Dir::Entry;
use strict;
use warnings;

=head1 Dir::Entry

File::CAS::Dir::Entry is a super-light-weight class.  More of an interface, really.

It has no public constructor, and will be constructed by a File::CAS::Dir object
or subclass.  The File::CAS::Dir::Entry interface contains the following read-only
accessors:

=head1 Dir::Entry ACCESSORS

=head2 name

The name of this entry within its directory.

The directory object should always return normal perl unicode strings, rather than
a string of raw bytes.  (if the raw filename wasn't a valid unicode string, it
should have been converted to values 0..255 in the unicode charset)

In other words, the name should always be platform-neutral.

=head2 type

One of "file", "dir", "symlink", "blockdev", "chardev", "pipe", "socket"

=head2 hash

The store's checksum of the data in the referenced file or directory.

This should by undef for any type other than 'file' or 'dir'

=head2 size

The size of the referenced file.  In the case of directories, this is the size of
the serialized directory.  All other types should be 0 or undef.

=head2 create_ts

The timestamp of the creation of the file, expressed in Unix Epoch seconds.

=head2 modify_ts

The timestamp the file was last modified, expressed in Unix Epoch seconds.

=head2 linkTarget

The target of a symbolic link, in platform-dependant path notation.

=head2 unix_uid

The number reported by lstat for uid.

=head2 unix_gid

The number reported by lstat for gid

=head2 unix_user

The user name corresponding to the unix_uid

=head2 unix_group

The group name corresponding to the unix_gid

=head2 unix_mode

The unix permissions for the entry, as reported by lstat.

=head2 unix_atime

The unix atime, as reported by lstat.

=head2 unix_ctime

The unix ctime, as reported by lstat.

=head2 unix_dev

The device file number, as reported by lstat.

=head2 unix_inode

The inode number, as reported by lstat.

=head2 unix_nlink

The the hardlink count reported by lstat.

=head2 unix_blocksize

The block size reported by lstat.

=head2 unix_blockcount

The block count reported by lstat.

=cut

use Scalar::Util 'reftype';

sub new {
	my $class= shift;
	my $hash= (scalar(@_) eq 1 && ref $_[0])? $_[0] : { @_ };
	bless \$hash, $class;
}

# We expect other subclasses to be based on different native objects, like arrays,
#  so we have a special accessor that only takes effect if it is a hashref, and
#  safely returns undef otherwise.
{ eval "sub $_ { \$_[0]->asHash->{$_} }; 1" or die "$@"
  for qw: name type hash size create_ts modify_ts linkTarget
	unix_uid unix_user unix_gid unix_group unix_mode unix_atime unix_ctime unix_mtime unix_dev unix_inode unix_nlink unix_blocksize unix_blocks :;
}

sub createDate { require DateTime; DateTime->from_epoch( epoch => $_[0]->create_ts ) }
sub modifyDate { require DateTime; DateTime->from_epoch( epoch => $_[0]->modify_ts ) }
sub asHash { ${$_[0]} }

1;