package DataStore::CAS::FS::Dir;
use 5.008;
use strict;
use warnings;
use Carp;
use Try::Tiny;

our $VERSION= 1.0000;

=head1 NAME

DataStore::CAS::FS::Dir - Object representing a directory of file entries,
indexed by filename.

=head1 SYNOPSIS

  my $dir= DataStore::CAS::FS::Dir->new(
    file => $cas_file,
    format => $codec_name,
    entries => \@entries,
    metadata => $metadata
  );

=head1 DESCRIPTION

Directory objects have a very basic API of being able to fetch an entry by
name (optionally case-insensitive, as the user chooses), and iterate all
entries.

Directory objects are B<IMMUTABLE>, as are the Dir::Entry objects they return.

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

=head1 METHODS

=head2 $class->new( %params | \%params )

Create a new basic Dir object.  The required parameters are 'file', and
'format'.  'metadata' will default to an empty hashref, and 'entries' will
default to an empty list.

The 'entries' parameter is not a public attribute, and is stored internally
as _entries.  This is because not all subclasses will have an array of entries
available.  Use the method "iterator" instead.

=cut

sub new {
	my $class= shift;
	my %p= (1 == @_ && ref $_[0] eq 'HASH')? %{$_[0]} : @_;
	defined $p{file} or croak "Attribute 'file' is required";
	defined $p{format} or croak "Attribute 'format' is required";
	$p{metadata} ||= {};
	$p{_entries}= delete $p{entries} || [];
	bless \%p, $class;
}

=head2 $dir->iterator

Returns an iterator over the entries in the directory.

The iterator is a coderef where each successive call returns the next
Dir::Entry.  Returns undef at the end of the list.
Entries are not guaranteed to be in any order, or even to be
unique names.  (in particular, because of case sensitivity rules)

=cut

sub iterator {
	my $list= $_[0]{_entries};
	my ($i, $n)= (0, scalar @$list);
	return sub { $i < $n? $list->[$i++] : undef };
}

=head2 $ent= $dir->get_entry($name, %flags)

Get a directory entry by name.

If flags{case_insensitive} is true, then the directory will attempt to do a
case-folding lookup on the given name.  Note that all directories are
case-sensitive when written, and the case-insensitive feature is meant to help
emulate Windows-like behavior.  In other words, you might have two entries
that differ only by case, and the caseless lookup will pick one arbitrarily.

=cut
sub get_entry {
	my ($self, $name, $flags)= @_;
	return $flags->{case_insensitive}?
		($self->{_entry_name_map_caseless} ||= do {
			my (%lookup, $ent, $iter);
			for ($iter= $self->iterator; defined ($ent= $iter->()); ) {
				$lookup{uc $ent->name}= $ent
			}
			\%lookup;
		})->{uc $name}
		:
		($self->{_entry_name_map} ||= do {
			my (%lookup, $ent, $iter);
			for ($iter= $self->iterator; defined ($ent= $iter->()); ) {
				$lookup{$ent->name}= $ent
			}
			\%lookup;
		})->{$name};
}

$INC{'DataStore/CAS/FS/Dir/Entry.pm'}= 1;
package DataStore::CAS::FS::Dir::Entry;
use strict;
use warnings;

=head1 Dir::Entry

DataStore::CAS::FS::Dir::Entry is a super-light-weight class.  More of an
interface, really.  The Dir::Entry interface contains the following read-only
accessors:

=head1 Dir::Entry ACCESSORS

=head2 name

The name of this entry within its directory.

The directory object should always return normal perl unicode strings, rather than
a string of raw bytes.  (if the raw filename wasn't a valid unicode string, it
should have been converted to codepoints 0..255)

In other words, the name should always be platform-neutral.

=head2 type

One of "file", "dir", "symlink", "blockdev", "chardev", "pipe", "socket".

Note that 'symlink' refers only to UNIX style symlinks.
As support for other systems' symbolic links is added, new type strings will
be added to this list, and the type will determine how to interpret the
C<ref> value.

Type must always be defined when stored, though instances with an undefined
type might exist temporarily while building a new filesystem.

=head2 ref

For file or dir: the store's checksum of the referenced data.

For symlink: the path as a string of path parts separated by '/'.

For blockdev and chardev: the device node as a string of "$major,$minor".

Ref is allowed to be undefined (regardless of type) if the data is not known.

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

# We expect other subclasses to be based on different native objects, like
#  arrays, so our accessor pulls from the 'as_hash' so that it can safely
#  return undef if the subclass doesn't support it.
BEGIN {
	eval "sub $_ { \$_[0]->as_hash->{$_} }; 1" or die $@
		for qw(
			name
			type
			ref
			size
			create_ts
			modify_ts
			unix_uid
			unix_user
			unix_gid
			unix_group
			unix_mode
			unix_atime
			unix_ctime
			unix_mtime
			unix_dev
			unix_inode
			unix_nlink
			unix_blocksize
			unix_blockcount
		);
}


=head1 Dir::Entry METHODS

=head2 new( %fields | \%fields )

The default constructor *uses* the hashref you pass to it. (it does not clone)
This should be ok, because the Dir::Entry objects should never be modified.
We don't yet enforce that though, so be careful what you pass to it.

A second oddity is that if you call "->new" on an object instead of a package,
it will supply all the fields of the object as defaults, and possibly not
return the same class as the original.

=cut

sub new {
	my $class= shift;
	my $hash= (@_ == 1 and CORE::ref $_[0] eq 'HASH')? $_[0] : { @_ };
	bless \$hash, $class;
}

=head2 clone( %overrides )

Create a new directory entry, with some fields overridden.

Most implementations will simply call C<new( %{$self->as_hash}, %overrides )>,
but in some implementations it might not be possible or practical to apply
the requested overrides, so you might get back a different class than the
original.

=cut

sub clone {
	my $self= shift;
	CORE::ref($self)->new(
		%{$self->as_hash},
		(@_ == 1 and CORE::ref $_[0] eq 'HASH')? @{$_[0]} : @_
	);
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