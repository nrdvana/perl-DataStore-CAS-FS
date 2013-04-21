package DataStore::CAS::FS::DirEnt;
use strict;
use warnings;

=head1 DirEnt

DataStore::CAS::FS::DirEnt is a super-light-weight class.  More of an
interface, really.  The DirEnt interface contains the following read-only
accessors:

=head1 DirEnt ACCESSORS

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


=head1 DirEnt METHODS

=head2 new( %fields | \%fields )

The default constructor *uses* the hashref you pass to it. (it does not clone)
This should be ok, because the DirEnt objects should never be modified.
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