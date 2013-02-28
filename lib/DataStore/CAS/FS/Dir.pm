package DataStore::CAS::FS::Dir;
use 5.008;
use strict;
use warnings;
use Carp;
use Try::Tiny;
require JSON;
require DataStore::CAS::FS::NonUnicode;

our $VERSION= 1.0000;

=head1 NAME

DataStore::CAS::FS::Dir - Object representing a directory of file entries,
indexed by filename.

=head1 SYNOPSIS

  my %metadata= ( foo => 1, bar => 42 );
  my @entries= ( { name => 'file1', type => 'file', ref => 'SHA1DIGESTVALUE', mtime => '1736354736' } );
  
  my $bytes= '';
  open my $handle, '>', \$bytes;
  DataStore::CAS::FS::Dir::Universal->encode(\@entries, $handle);
  
  open $handle, '<', \$bytes;
  my $dir= DataStore::CAS::FS::Dir->decode($handle);
  
  print Dumper( $dir->get_entry('file1') );

=head1 DESCRIPTION

This class handles the job of packing or unpacking a directory listing to/from
a stream of bytes.  This class can store any arbitrary metadata about a file,
and encodes it with JSON for compatibility with other tools.

Various subclasses exist which support more limited attributes and more
efficient encoding.

=over

=item Minimal

DataStore::CAS::FS::Dir::Minimal only stores filename, content reference, and
mtime, and results in a very compact serialization.  Use that one if you don't
care about permissions and just want enough information for an incremental
backup.

=item Unix

DataStore::CAS::FS::Dir::Unix stores bare 'stat' entries for each file.
It isn't so rigid as to use fixed-width fields, so it should serve any
unix-like architecture with similar stat() fields.

=item Others...

Eventually there will also be a ::Dir::UnixAttr if you want to
store ACLs and Extended Attributes, a ::Dir::DosFat for fat16/32, and a
::Dir::Windows for ACL-based Windows permissions.  Patches welcome.

=item Your Own

It is very easy to write your own directory serializer!  See the section
on L<DataStore::CAS::FS::Dir/EXTENDING>.

=back

Note that with the public API, you cannot instantiate DataStore::CAS::FS::Dir
objects until they have been serialized and deserialized again. To do so, build
a directory listing in the format returned by DataStore::CAS::FS::Scanner, and
then serialize it using an appropriate Directory class's encode() method.

All ::Dir objects are intended to be immutable.  They are also cached by
DataStore::CAS::FS, so modifying them could cause problems.  Don't do that.

=head2 Unicode

While I almost went with the option to operate purely on bytes, I decided to
bite the bullet and make this API as Unicode-friendly as I could.  The one
exception is filenames; Unix uses byte-oriented filenames, and Perl matches
this by auto-encoding all unicode strings to UTF-8 before accessing the
filesystem.  Since concatenating unicode with non-unicode in Perl will
generally mangle filenames, I decided to keep all the filenames as bytes.
Also, since 'ref' is either a Digest hash or a symlink value, it made sense
to keep it bytes-only as well.

All other metadata, however, should be unicode.  Most metadata related to
file entries is integer-based anyway, so this shouldn't be much of an issue.
If you need for some reason to attach a string of bytes and want it to come
back to Perl as bytes, wrap your data with the utility class
L<DataStore::CAS::Dir::NonUnicode>.  

=head1 ATTRIBUTES

=head2 file

Read-only, Required.  The DataStore::CAS::File this directory was deserialized
from.

=head2 store

Alias for file->store

=head2 hash

Alias for file->hash

=head2 size

Alias for file->size

=head2 format

The format string that identifies this directory encoding.

=head2 metadata

A hashref of arbitrary name/value pairs attached to the directory at the time
it was written.  DO NOT MODIFY.  (In the future, this might be protected by
Perl's internal const mechanism)

=cut

sub file     { $_[0]{file} }
sub store    { $_[0]{file}->store }
sub hash     { $_[0]{file}->hash }
sub size     { $_[0]{file}->size }

sub format   { $_[0]{format} }

sub metadata { $_[0]{metadata} } 

=head1 FACTORY FUNCTIONS

=head2 $class->RegisterFormat( $format => $dir_class )

Registers a directory format to be available to the factory-like 'new' method
of DataStore::CAS::FS::Dir.

While the system could have been designed to auto-load classes on demand, that
seemed like a bad idea because it would allow the contents of the CAS to load
perl modules.  With this design, you mist load all modules you wish to enable
before browsing the CAS.  All the directory modules in the standard
distribution of DataStore::CAS are enabled by default.

Typically the $format string is the same as the name of the $dir_class.
Directory modules in DataStore::CAS::FS::Dir leave off that package prefix,
so the empty string '' refers to DataStore::CAS::FS::Dir, and so on.

=cut
our %_Formats= ( '' => __PACKAGE__ );
sub RegisterFormat {
	my ($class, $format, $decoder_class)= @_;
	$decoder_class->isa($class)
		or croak "$decoder_class must inherit from $class";
	$_Formats{$format}= $decoder_class;
}

=head2 $class->new( $file | \%params )

This factory method reads the first few bytes of $file (which must be an
instance of DataStore::CAS::File) to determine which type of object to create.

The selected directory class's constructor will then be called.

The method can be called with just the file, or with a hashref of parameters.

Parameters:

=over

=item file

The single $file is equivalent to "{ file => $file }".  It specifies the CAS
item to read the serialized directory from.

=item format

If you know the format ahead of time, you may specify it to prevent new() from
needing to read the $file.  (though most directory classes will immediately
read it anyway)

format must be one of the registered formats.  See RegisterFormat.

=item handle

If you already opened the file for some reason, you can let the directory
re-use your handle.  Be warned that the directory will seek to the start of
the file first.  Also beware that some directory implementations might hold
onto the handle and seek around on it during calls to other methods.

=item data

If you already have the full data of the $file, you can pass it to prevent any
filesystem activity.  You might choose this if you were trying to use the
library in a non-blocking or event driven application.

=back

=cut

sub new {
	my $class= shift;
	my %p= (@_ == 1)? ((ref $_[0] eq 'HASH')? %{$_[0]} : ( file => $_[0] )) : @_;

	defined $p{file} or croak "Missing required attribute 'file'";
	defined $p{format} or $p{format}= $class->_read_format(\%p);

	# Once we get the name of the format, we can jump over to the constructor
	# for the appropriate class
	$class= $_Formats{$p{format}}
		or croak "Unknown directory format '$p{format}' in ".$p{file}->hash
			."\n(be sure to load relevant modules)";

	$_Formats{$p{format}}->_ctor(\%p);
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

sub _entry_list_cmp {
	my $a_n= $a->name;
	utf8::encode($a_n) if utf8::is_utf8($a_n);
	my $b_n= $b->name;
	utf8::encode($b_n) if utf8::is_utf8($b_n);
	return $a_n cmp $b_n;
}
sub SerializeEntries {
	my ($class, $entry_list, $metadata)= @_;
	ref($metadata) eq 'HASH' or croak "Metadata must be a hashref"
		if $metadata;

	my @entries= sort { $a->{name} cmp $b->{name} }
		map {
			my %entry= %{ref $_ eq 'HASH'? $_ : $_->as_hash};
			# Convert all name strings down to plain bytes, for our sort
			# (they should be already)
			utf8::encode($entry{name})
				if !ref $entry{name} and utf8::is_utf8($entry{name});
			\%entry;
		} @$entry_list;

	my $enc= JSON->new->utf8->canonical->convert_blessed;
	my $json= $enc->encode($metadata || {});
	my $ret= "CAS_Dir 00 \n"
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

=head2 $dir->iterator

Returns an iterator object with methods of '->next' and '->eof'.

Calling 'next' returns a Dir::Entry object, or undef if at the end of the
directory.  Entries are not guaranteed to be in any order, or even to be
unique names.  (in particular, because of case sensitivity rules)

=cut

sub iterator {
	return DataStore::CAS::FS::Dir::EntryIter->new($_[0]{_entries});
}

=head2 $ent= $dir->get_entry($name)

Get a directory entry by name.  The name is case-sensitive.
(Expect to see a second parameter '\%flags' sometime in the future)

=cut
sub get_entry {
	return (
		($_[0]{_entry_name_map} ||= { map { $_->name => $_ } $_[0]{_entries} })
			->{$_[1]}
	);
}

=head1 EXTENDING

Subclasses likely need to supply their own implementation of the following
methods:

=over

=item SerializeEntries

=item iterator

=item get_entry

=item _deserialize

=back

and call I<RegisterEntries> in a BEGIN block.

Subclasses have the following handy private functions to work with:

=head2 $class->_ctor( \%params )

Private-ish constructor.  Like "new" with no error checking, and requires a
blessable hashref.

Required parameters are "file" and "format".  Format must be the type encoded
in the file, or deserialization will fail.  The factory method Dir::new() will
usually pass a "handle" or "data" as well.

=cut

our @_ctor_params= qw: file format handle data :;
sub _ctor {
	my ($class, $params)= @_;
	my $self= { map { $_ => delete $params->{$_} } @_ctor_params };
	croak "Invalid param(s): ".join(', ', keys %$params)
		if keys %$params;
	my %args;
	@args{'handle','data'}= delete @{$self}{'handle','data'};
	bless $self, $class;
	$self->_deserialize(\%args);
	return $self;
}

=head2 $self->_deserialize( \%params )

This method is called at the end of the default constructor.

Attributes I<file> and I<format> have already been initialized, and if the
parameters 'handle' and 'data' were specified they are forwarded to this
method.

"data" is the complete data of the file, and if present should eliminate the
need to open the file.

"handle" is an open file handle to the data of the file, and should be used
if provided.

If neither is given, call file->open to get a handle to work with.  You also
have the option to lazy-open the file, and/or store the 'data' and 'handle'
for later use.

=cut

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

	my $enc= JSON->new()->utf8->canonical->convert_blessed
		->filter_json_single_key_object(
			'*NonUnicode*' => \&DataStore::CAS::FS::NonUnicode::FROM_JSON
		);
	my $data= $enc->decode($bytes);
	$self->{metadata}= $data->{metadata} or croak "Directory data is missing 'metadata'";
	$data->{entries} or croak "Directory data is missing 'entries'";
	my @entries;
	for my $ent (@{$data->{entries}}) {
		# While name and ref are probably logically unicode, we want them
		#  kept as octets for compatibility reasons.
		utf8::encode($ent->{name})
			if !ref $ent->{name} and utf8::is_utf8($ent->{name});
		utf8::encode($ent->{ref})
			if !ref $ent->{ref}  and utf8::is_utf8($ent->{ref});

		push @entries, DataStore::CAS::FS::Dir::Entry->new($ent);
	};
	$self->{_entries}= \@entries;
	$self;
}

=head2 $class->_magic_number

Returns a string that all serialized directories start with.
This is a constant and should never change.

=head2 $class->_calc_header_length( $format )

The header length is directly determined by the format string.
This method returns the header length in bytes.  A directory's encoded data
begins at this offset.

=cut

my $_MagicNumber= 'CAS_Dir ';

sub _magic_number { $_MagicNumber }

sub _calc_header_length {
	my ($class, $format)= @_;
	# Length of sprintf("CAS_Dir %02X %s\n", length($format), $format)
	return length($format)+length($_MagicNumber)+4;
}

=head2 $class->_read_format( \%params )

This method inspects the first few bytes of $params->{file} to read the format
string, which it returns.  It first uses $params->{data} if available, or
$params->{handle}, or if neither is available it opens a new handle to the
file which it returns in $params.

=cut

sub _read_format {
	my ($class, $params)= @_;

	# The caller is allowed to pre-load the data so that we don't need to read it here.
	my $buf= $params->{data};
	# If they didn't, we need to load it.
	if (!defined $params->{data}) {
		$params->{handle}= $params->{file}->open
			unless defined $params->{handle};
		seek($params->{handle}, 0, 0) or croak "seek: $!";
		$class->_readall($params->{handle}, $buf, length($_MagicNumber)+2);
	}

	# first 8 bytes are "CAS_Dir "
	# Next 2 bytes are the length of the format in uppercase ascii hex (limiting format id to 255 characters)
	substr($buf, 0, length($_MagicNumber)) eq $_MagicNumber
		or croak "Bad magic number in directory ".$params->{file}->hash;
	my $format_len= hex substr($buf, length($_MagicNumber), 2);

	# Now we know how many additional bytes we need
	if (!defined $params->{data}) {
		$class->_readall($params->{handle}, $buf, 1+$format_len+1, length($buf));
	}

	# The byte after that is a space character.
	# The format id string follows, in exactly $format_len bytes
	# There is a newline (\n) at the end of the format string which is not part of that count.
	substr($buf, length($_MagicNumber)+2, 1) eq ' '
		and substr($buf, length($_MagicNumber)+3+$format_len, 1) eq "\n"
		or croak "Invalid directory encoding in ".$params->{file}->hash;
	return substr($buf, length($_MagicNumber)+3, $format_len);
}

=head2 $class->_readall( $handle, $buf, $count, $offset )

A small wrapper around 'read()' which croaks if it can't read the full
requested number of bytes, and properly handles EINTR and EAGAIN and
partial reads.

=cut

sub _readall {
	my $got= read($_[1], $_[2], $_[3], $_[4]||0);
	return $got if defined $got and $got == $_[3];
	my $count= $_[3];
	while (1) {
		if (defined $got) {
			croak "unexpected EOF"
				unless $got > 0;
			$count -= $got;
		}
		else {
			croak "read: $!"
				unless $!{EINTR} || $!{EAGAIN};
		}
		$got= read($_[1], $_[2], $count, length $_[2]);
	}
}


package DataStore::CAS::FS::Dir::EntryIter;
use strict;
use warnings;

=head2 DataStore::CAS::FS::Dir::EntryIter->new( \@entries )

This is a very simple iterator class which takes an arrayref of Dir::Entry
and iterates it.  If you have your directory entries in an array, this
makes a nice on-line implementation of the ->iterator method.

Don't bother trying to subclass it; custom iterators don't need a class
hierarchy, just a 'next' and 'eof' method.

=cut

sub new {
	bless { entries => $_[1], i => 0, n => scalar( @{$_[1]} ) }, $_[0];
}

sub next {
	return $_[0]{entries}[ $_[0]{i}++ ]
		if $_[0]{i} < $_[0]{n};
	return undef;
}

sub eof {
	return $_[0]{i} >= $_[0]{n};
}

$INC{'DataStore/CAS/FS/Dir/Entry.pm'}= 1;
package DataStore::CAS::FS::Dir::Entry;
use strict;
use warnings;

=head1 Dir::Entry

DataStore::CAS::FS::Dir::Entry is a super-light-weight class.  More of an
interface, really.

It has no public constructor, and will be constructed by a Dir object or
subclass.  The Dir::Entry interface contains the following read-only
accessors:

=head1 Dir::Entry ACCESSORS

=head2 name

The name of this entry within its directory.

The directory object should always return normal perl unicode strings, rather than
a string of raw bytes.  (if the raw filename wasn't a valid unicode string, it
should have been converted to values 0..255 in the unicode charset)

In other words, the name should always be platform-neutral.

=head2 type

One of "file", "dir", "symlink", "blockdev", "chardev", "pipe", "socket".

Note that 'symlink' refers only to UNIX style symlinks.
As support for other systems' symbolic links is added, new type strings will
be added to this list, and the type will determine how to interpret the
C<ref> value.

=head2 ref

The store's checksum of the data in the referenced file or directory.

For symlink, the path.  For blockdev and chardev, the device node.

=head2 size

The size of the referenced file.  In the case of directories, this is the size of
the serialized directory.  All other types should be 0 or undef.

=head2 create_ts

The timestamp of the creation of the file, expressed in Unix Epoch seconds.

=head2 modify_ts

The timestamp the file was last modified, expressed in Unix Epoch seconds.

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

=head1 Dir::Entry METHODS

=head2 new(\%hash)

The default constructor *uses* the hashref you pass to it. (it does not clone)
This should be ok, because the Dir::Entry objects should never be modified.
We don't yet enforce that though, so be careful what you pass to it.

=cut

sub new {
	my $class= shift;
	my $hash= (scalar(@_) eq 1 && ref $_[0] eq 'HASH')? $_[0] : { @_ };
	bless \$hash, $class;
}

# We expect other subclasses to be based on different native objects, like
#  arrays, so our accessor pulls from the 'as_hash' so that it can safely
#  return undef if the subclass doesn't support it.
{ eval "sub $_ { \$_[0]->as_hash->{$_} }; 1" or die $@
  for qw: name type ref size create_ts modify_ts
	unix_uid unix_user unix_gid unix_group unix_mode unix_atime unix_ctime
	unix_mtime unix_dev unix_inode unix_nlink unix_blocksize unix_blocks :;
}

=head2 create_date

Convenience method.  Creates a DateTime object from the create_ts field.
Returns undef if create_ts is undef.

=head2 modify_date

Convenience method.  Creates a DateTime object from the modify_ts field.
Returns undef if modify_ts is undef.

=cut

sub create_date {
	require DateTime;
	return defined $_[0]->create_ts?
		DateTime->from_epoch( epoch => $_[0]->create_ts )
		: undef
}
sub modify_date {
	require DateTime;
	return defined $_[0]->modify_ts?
		DateTime->from_epoch( epoch => $_[0]->modify_ts )
		: undef
}

=head2 as_hash

Returns the fields of the directory entry as a hashref.  The hashref will
contain only the public fields.  The hashref SHOULD NEVER BE MODIFIED.
(Future versions might use perl's internals to force the hashref to be
constant)

=cut

sub as_hash { ${$_[0]} }

1;