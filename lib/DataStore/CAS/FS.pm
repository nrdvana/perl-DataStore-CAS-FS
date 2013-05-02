package DataStore::CAS::FS;
use 5.008;
use Moo 1.000007;
use Carp;
use Try::Tiny 0.11;
use File::Spec 3.33;
use DataStore::CAS 0.01;

our $VERSION= '0.0100';

require DataStore::CAS::FS::Dir;
require DataStore::CAS::FS::DirCodec::Universal;
require DataStore::CAS::FS::DirCodec::Minimal;
require DataStore::CAS::FS::DirCodec::Unix;

# ABSTRACT: Virtual Filesystem backed by Content-Addressable Storage

=head1 SYNOPSIS

  # Create a new empty filesystem
  my $casfs= DataStore::CAS::FS->new(
    store => DataStore::CAS::Simple->new(
      path => './foo/bar',
      create => 1,
      digest => 'SHA-256'
    )
  );
  
  # Open an existing root directory on an existing store
  $casfs= DataStore::CAS::FS->new( store => $cas, root_dir => $digest_hash );
  
  # --- These pass through to the $cas module
  
  $hash= $casfs->put("Blah"); 
  $hash= $casfs->put_file("./foo/bar/baz");
  $file= $casfs->get($hash);
  
  # Open a path within the filesystem
  $handle= $casfs->path('1','2','3','myfile')->open;
  
  # Make some changes
  $casfs->apply_path(['1', '2', 'myfile'], { ref => $some_new_file });
  $casfs->apply_path(['1', '2', 'myfile_copy'], { ref => $some_new_file });
  # Commit them
  $casfs->commit();
  
=head1 DESCRIPTION

DataStore::CAS::FS extends the content-addressable API to support directory
objects which let you store store traditional file hierarchies in the CAS,
and look up files by a path name (so long as you know the hash of the root).

The methods provided allow you to traverse the virtual directory hierarchy,
make changes to it, and commit the changes to create a new filesystem
snapshot.  The L<DataStore::CAS> backend provides readable and seekable file
handles.  There is *not* any support for access control, since those
concepts are system dependent.  The module DataStore::CAS::FS::Fuse (not yet
written) will have an implementation of permission checking appropriate for
Unix.

The directories can contain arbitrary metadata, making them suitable for
backing up filesystems from Unix, Windows, or other environments.
You can also pick directory encoding plugins to more efficiently encode
just the metadata you care about.

Each directory is serialized into a file which is stored in the CAS like any
other, resulting in a very clean implementation.  You cannot determine whether
a file is a directory or not without the context of the containing directory,
and you need to know the digest hash of the root directory in order to browse
the full filesystem.  On the up side, you can store any number of filesystems
in one CAS by maintaining a list of roots.

The root's digest hash is affected by all the content of the entire tree, so
the root hash will change each time you alter any directory in the tree.  But,
any unchanged files in that tree will be re-used, since they still have the
same digest hash.  You can see great applications of this design in a number
of version control systems, notably Git.

=head1 ATTRIBUTES

=head2 store

Read-only.  An instance of a class implementing L<DataStore::CAS>.

=head2 root_entry

A L<DataStore::CAS::DirEnt> object describing the root of the tree.
Must be of type "dir".  Should have a name of "", but not required.
You can pick an arbitrary directory for a chroot-like-effect, but beware
of broken symlinks.

C<root_entry> refers to an **immutable** directory.  If you make in-memory
overrides to the filesystem using C<apply_path> or the various convenience
methods, C<root_entry> will continue to refer to the original static
filesystem. If you then C<commit()> those changes, C<root_entry> will be
updated to refer to the new filesystem.

You can create a list of filesystem snapshots by saving a copy of root_entry
each time you call C<commit()>.  They will all continue to exist within the
CAS.  Cleaning up the CAS is left as an exercise for the reader. (though
utility methods to help with this are in the works)

=head2 case_insensitive

Read-only.  Defaults to false.  If set to true in the constructor, this causes
all directory entries to be compared in a case-insensitive manner, and all
directory objects to be loaded with case-insensitive lookup indexes.

=head2 hash_of_null

Read-only.  Passes through to store->hash_of_null

=head2 hash_of_empty_dir

This returns the canonical digest hash for an empty directory.
In other words, the return value of

  put_scalar( DataStore::CAS::FS::DirCodec::Minimal->encode([],{}) ).

This value is cached for performance.

It is possible to encode empty directories with any plugin, so not all empty
directories will have this key, but any time the library knows it is writing
an empty directory, it will use this value instead of recalculating the hash
of an empty dir.

=head2 dir_cache

Read-only.  A DataStore::CAS::FS::DirCache object which holds onto recently
used directory objects.  This object can be used in multiple CAS::FS objects
to make the most of the cache.

=cut

has store             => ( is => 'ro', required => 1, isa => \&_validate_cas );
has root_entry        => ( is => 'rwp', required => 1 );
has case_insensitive  => ( is => 'ro', default => sub { 0 } );

sub hash_of_null         { $_[0]->store->hash_of_null }
has hash_of_empty_dir => ( is => 'lazy' );

has dir_cache         => ( is => 'rw', default => sub { DataStore::CAS::FS::DirCache->new() } );

# _path_overrides is a tree of nodes, each of the form:
# $node= {
#   entry   => $Dir_Entry,  # mandatory
#   dir     => $CAS_FS_Dir, # optional, created on demand
#   subtree => {
#     KEY1 => $node1,
#     KEY2 => $node2,
#     ...
#   }
# }
#
#  If 'case_insensitive' is true, the keys will all be upper-case, but the $Dir_Entry
#  objects will contain the correct-case name.
#
has _path_overrides   => ( is => 'rw' );

=head1 METHODS

=head2 new

  $fs= $class->new( %args | \%args )

Parameters:

=over

=item store - required

An instance of (a subclass of) L<DataStore::CAS>

=item root_entry - required

An instance of L<DataStore::CAS::FS::DirEnt>, or a hashref of DirEnt fields,
or an empty hash if you want to start from an empty filesystem, or a
L<DataStore::CAS::FS::Dir> which you want to be the root directory
(or a L<DataStore::CAS::File> object that contains a serialized Dir) or
or a digest hash of that File within the store.

=item root - alias for root_entry

=back

=cut

sub _build_hash_of_empty_dir {
	my $self= shift;
	my $empty= DataStore::CAS::FS::DirCodec::Minimal->encode([],{});
	return $self->store->put_scalar($empty);
}

sub _validate_cas {
	my $cas= shift;
	ref($cas) && ref($cas)->can('get') && ref($cas)->can('put')
		or croak "Invalid CAS object: $cas"
};

sub BUILDARGS {
	my $class= shift;
	my %p= (@_ == 1 && ref $_[0] eq 'HASH')? %{$_[0]} : @_;
	# Root is an alias for root_entry
	if (defined $p{root}) {
		defined $p{root_entry}
			and croak "Specify only one of 'root' or 'root_entry'";
		$p{root_entry}= delete $p{root};
	}
	return \%p;
}

sub BUILD {
	my ($self, $args)= @_;
	my @invalid= grep { !$self->can($_) } keys %$args;
	croak "Invalid param(s): ".join(', ', @invalid)
		if @invalid;

	croak "Missing/Invalid parameter 'dir_cache'"
		unless defined $self->dir_cache and $self->dir_cache->can('clear');

	# coerce root_entry to an actual DirEnt object
	my $root= $self->root_entry;
	defined $root
		or croak "root_entry is required";
	unless (ref $root && ref($root)->isa('DataStore::CAS::FS::DirEnt')) {
		$self->_set_root_entry(
			DataStore::CAS::FS::DirEnt->new({
				type => 'dir',
				name => '',
				# Assume scalars are digest_hash values.
				!ref $root? ( ref => $root )
					# Hashrefs might be empty, to indicate an empty directory
					: ref $root eq 'HASH'? ( ref => $self->hash_of_empty_dir, %$root )
					# Is it a ::File or ::Dir object?
					: ref($root)->can('hash')? ( ref => $root->hash )
					# Else take a guess that it is a digest_hash wrapped in an object
					: ( ref => "$root" )
			})
		);
	}
	croak "Invalid parameter 'root_entry'"
		unless ref $self->root_entry
			and ref($self->root_entry)->can('type')
			and $self->root_entry->type eq 'dir'
			and defined $self->root_entry->ref;
	# If they gave us a 'root_entry', make sure we can load it
	$self->get_dir($self->root_entry->ref)
		or croak "Unable to load root directory '".$self->root_entry->ref."'";
}

=head2 get

Alias for L<DataStore::CAS/get|store->get>

=cut

sub get {
	(shift)->store->get(@_);
}

=head2 get_dir

  $dir= $fs->get_dir( $digest_hash );
  $dir= $fs->get_dir( $fileObj );
  $dir= $fs->get_dir( $either, \%flags );

This returns a de-serialized directory object found by its hash.  It is a
shorthand for 'get' on the Store, and deserializing enough of the result to
create a usable L<DataStore::CAS::FS::Dir> object (or subclass).

Also, this method caches recently used directory objects, since they are
immutable. (but woe to those who break the API and modify their directory
objects!)

Returns undef if the digest hash isn't in the store, but dies if an error
occurs while decoding one that exists.

=cut

sub get_dir {
	my ($self, $hash_or_file, $flags)= @_;
	my ($hash, $file)= (ref $hash_or_file and $hash_or_file->can('hash'))
		? ( $hash_or_file->hash, $hash_or_file )
		: ( $hash_or_file, undef );
	
	my $dir= $self->dir_cache->get($hash);
	return $dir if defined $dir;
	
	# Return undef if the directory doesn't exist.
	return undef
		unless defined ($file ||= $self->store->get($hash));
	
	# Deserialize directory.  This can throw exceptions if it isn't a valid encoding.
	$dir= DataStore::CAS::FS::DirCodec->load($file);
	# Cache it
	$self->dir_cache->put($dir);
	return $dir;
}

=head2 put

Alias for L<DataStore::CAS/put|store->put>

=head2 put_scalar

Alias for L<DataStore::CAS/put_scalar|store->put_scalar>

=head2 put_file

Alias for L<DataStore::CAS/put_file|store->put_file>

=head2 put_handle

Alias for L<DataStore::CAS/put_handle|store->put_handle>

=head2 validate

Alias for L<DataStore::CAS/validate|store->validate>

=cut

sub put        { (shift)->store->put(@_) }
sub put_scalar { (shift)->store->put_scalar(@_) }
sub put_file   { (shift)->store->put_file(@_) }
sub put_handle { (shift)->store->put_handle(@_) }
sub validate   { (shift)->store->validate(@_) }

=head2 path

  $path= $fs->path( @path_names )

Returns a L</"PATH OBJECTS"|DataStore::CAS::FS::Path> object which provides
frendly object-oriented access to several other methods of CAS::FS. This
object does *nothing* other than curry parameters, for your convenience.
In particular, the path isn't resolved until you try to use it, and might not
be valid.

See L</resolve_path> for notes about @path_names.  Especially note that your
path needs to start with the volume name, which will usually be ''.  Note that
you get this already if you take an absolute path and pass it to
L<File::Spec/splitdir|File::Spec->splitdir>.

=cut

sub path {
	bless { filesystem => (shift), path_names => [ @_ ] },
		'DataStore::CAS::FS::Path';
}

=head2 resolve_path

  $path_array= $fs->resolve_path( \@path_names )
  $path_array= $fs->resolve_path( \@path_names, \%flags )

Returns an arrayref of L<DataStore::CAS::FS::DirEnt> objects corresponding
to the canonical absolute specified path, starting with the C<root_entry>.

First, a note on @path_names: you need to specify the volume, which for UNIX
is the empty string ''.  While volumes might seem like an unnecessary
concept, and I wasn't originally going to include that in my design, it helped
in 2 major ways: it allows us to store a regular ::DirEnt for the root
directory (which is useful for things like permissions and timestamp) and
allows us to record general metadata for the filesystem as a whole, within the
->metadata of the volume_dir.  As a side benefit, Windows users might
appreciate being able to save backups of multiple volumes in a way that
preserves their view of the system.  As another side benefit, it is compatible
with L<< File::Spec/splitdir|File::Spec->splitdir >>.

Next, a note on resolving paths: This function will follow symlinks in much
the same way Linux does.  If the path you specify ends with a symlink, the
result will be a DirEnt describing the symlink.  If the path you specify
ends with a symlink and a "" (equivalent of ending with a '/'), the symlink
will be resolved to a DirEnt for the target file or directory. (and if
it doesn't exist, you get an error)

Also, its worth noting that the directory objects in DataStore::CAS::FS are
strictly a tree, with no back-reference to the parent directory.  So, ".."
in the path will be resolved by removing one element from the path.  HOWEVER,
this still gives you a kernel-style resolve (rather than a shell-style resolve)
because if you specify "/1/foo/.." and foo is a symlink to "/1/2/3",
the ".." will back you up to "/1/2/" and not "/1/".

The tree-with-no-parent-reference design is also why we return an array of
the entire path, since you can't take a final directory and trace it backwards.

If the path does not exist, or cannot be resolved for some reason, this method
will either return undef or die, based on whether you provided the optional
'nodie' flag.

Flags:

=over

=item no_die => $bool

Return undef instead of dying

=item error_out => \$err_variable

If set to a scalar-ref, the scalar ref will receive the error message, if any.
You probably want to set 'nodie' as well.

=item partial => $bool

If the path doesn't exist, any missing directories will be given placeholder
DirEnt objects.  You can test whether the path was resolved completely by
checking whether $result->[-1]->type is defined.

=item mkdir => 1 || 2

If mkdir is 1, missing directories will be created on demand.

If mkdir is 2, 

=back

=cut

sub resolve_path {
	my ($self, $path, $flags)= @_;
	$flags ||= {};
	
	my $ret= $self->_resolve_path($path, { follow_symlinks => 1, %$flags });
	
	# Array means success, scalar means error.
	if (ref($ret) eq 'ARRAY') {
		# The user wants directory entries, not "nodes".
		$_= $_->{entry} for @$ret;
		return $ret;
	}

	# else, got an error...
	${$flags->{error_out}}= $ret
		if ref $flags->{error_out};
	croak $ret unless $flags->{no_die};
	return undef;
}

sub _resolve_path {
	my ($self, $path, $flags)= @_;

	my @path= ref($path)? @$path : File::Spec->splitdir($path);
	my @nodes= ( $self->_path_overrides || { entry => $self->root_entry } );
	
	return "Root directory must be a directory"
		unless $nodes[0]{entry}->type eq 'dir';

	my @mkdir_defaults= %{$flags->{mkdir_defaults}}
		if ref $flags->{mkdir_defaults};
	push @mkdir_defaults, type => 'dir', ref => undef;

	while (@path) {
		my $ent= $nodes[-1]{entry};
		my $dir;

		# Support for "symlink" is always UNIX-based (or compatible)
		# As support for other systems' symbolic paths are added, they
		# will be given unique '->type' values, and appropriate handling.
		if ($ent->type eq 'symlink' and $flags->{follow_symlinks}) {
			# Sanity check on symlink entry
			my $target= $ent->ref;
			defined $target and length $target
				or return 'Invalid symbolic link "'.$ent->name.'"';

			unshift @path, split('/', $target, -1);
			pop @nodes;
			
			# If an absolute link, we start over from the root
			@nodes= ( $nodes[0] )
				if $path[0] eq '';

			next;
		}

		if ($ent->type ne 'dir') {
			return 'Cannot descend into directory entry "'.$ent->name.'" of type "'.$ent->type.'"'
				unless ($flags->{mkdir}||0) > 1;
			# Here, mkdir flag converts entry into a directory
			$nodes[-1]{entry}= $ent->clone(@mkdir_defaults);
		}

		# Get the next path component, ignoring empty and '.'
		my $name= shift @path;
		next unless defined $name and length $name and ($name ne '.');

		# We handle '..' procedurally, moving up one real directory and *not* backing out of a symlink.
		# This is the same way the kernel does it, but perhaps shell behavior is preferred...
		if ($name eq '..') {
			return "Cannot access '..' at root directory"
				unless @nodes > 1;
			pop @nodes;
			next;
		}

		# If this directory has an in-memory override for this name, use it
		my $subnode;
		if ($nodes[-1]{subtree}) {
			my $key= $self->case_insensitive? uc $name : $name;
			$subnode= $nodes[-1]{subtree}{$key};
		}
		if (!defined $subnode) {
			# Else we need to find the name within the current directory

			# load it if it isn't cached
			if (!defined $nodes[-1]{dir} && defined $ent->ref) {
				defined ( $nodes[-1]{dir}= $self->get_dir($ent->ref) )
					or return 'Failed to open directory "'.$ent->name.' ('.$ent->ref.')"';
			}

			# If we're working on an available directory, try loading it
			my $subent= $nodes[-1]{dir}->get_entry($name)
				if defined $nodes[-1]{dir};
			$subnode= { entry => $subent }
				if defined $subent;
		}

		# If we haven't found one, or if it is 0 (deleted), either create or die.
		if (!$subnode) {
			# If we're supposed to create virtual entries, do so
			if ($flags->{mkdir} or $flags->{partial}) {
				$subnode= {
					entry => DataStore::CAS::FS::DirEnt->new(
						name => $name,
						# It is a directory if there are more path components to resolve.
						(@path? @mkdir_defaults : ())
					)
				};
			}
			# Else it doesn't exist and we fail.
			else {
				my $dir_path= File::Spec->catdir(map { $_->{entry}->name } @nodes);
				return "Directory \"$dir_path\" is not present in storage"
					unless defined $nodes[-1]{dir};
				return "No such directory entry \"$name\" at \"$dir_path\"";
			}
		}

		push @nodes, $subnode;
	}
	
	\@nodes;
}

=head2 set_path

  $fs->set_path( \@path, $Dir_Entry )
  $fs->set_path( \@path, $Dir_Entry, \%flags )
  # always returns '1'

Temporarily override a directory entry at @path.  If $Dir_Entry is false, this
will cause @path to be unlinked.  If the name of Dir_Entry differs from the
final component of @path, it will act like a rename (which is the same as just
unlinking the old path and creating the new path)  If Dir_Entry is missing a
name, it will default to the final element of @path.

C<path> may be either an arrayref of names, or a string which will be split by
L<File::Spec>.

$Dir_Entry is either an instance of L<DataStore::CAS::FS::DirEnt>, or a
hashref of the fields to create one.

No fields of the old dir entry are used; if you want to preserve some of them,
you need to do that yourself (see L<DataStore::CAS::FS::DirEnt/clone|clone>)
or use the C<update_path()> method.

If @path refers to nonexistent directories, they will be created as with a
virtual "mkdir -p", and receive the default metadata of
C<$flags{default_dir_fields}> (by default, nothing)  If $path travels through
a non-directory (aside from symlinks, unless C<$flags{follow_symlinks}> is set
to 0) this will throw an exception, unless you specify C<$flags{force_create}>
which causes an offending directory entry to be overwritten by a new
subdirectory.

Note in particluar that if you specify

  apply_path( "/a_symlink/foo", $Dir_Entry, { follow_symlinks => 0, force_create => 1 })

"a_symlink" will be deleted and replaced with an actual directory.

None of the changes from apply_path are committed to the CAS until you call
C<commit()>.  Also, C<root_entry> does not change until you call C<commit()>,
though the root entry shown by L</resolve_path> does.

You can return to the last committed state by calling C<rollback()>, which is
conceptually equivalent to C<< $fs= DataStore::CAS::FS->new( $fs->root_entry ) >>.

=cut

sub set_path {
	my ($self, $path, $newent, $flags)= @_;
	$flags ||= {};
	my $nodes= $self->_resolve_path($path, { follow_symlinks => 1, partial => 1, %$flags });
	croak $nodes unless ref $nodes;

	# replace the final entry, after applying defaults
	if (!$newent) {
		$newent= 0; # 0 means unlink
	} elsif (ref $newent eq 'HASH' or !defined $newent->name or !defined $newent->type) {
		my %ent_hash= %{ref $newent eq 'HASH'? $newent : $newent->as_hash};
		$ent_hash{name}= $nodes->[-1]{entry}->name
			unless defined $ent_hash{name};
		defined $ent_hash{name} && length $ent_hash{name}
			or die "No name for new dir entry";
		$ent_hash{type}= $nodes->[-1]{entry}->type || 'file'
			unless defined $ent_hash{type};
		$newent= DataStore::CAS::FS::DirEnt->new(\%ent_hash);
	}
	$nodes->[-1]{entry}= $newent;
	$self->_apply_overrides($nodes);
}

=head2 update_path

  $fs->update_path( \@path, \%changes, \%flags )
  $fs->update_path( \@path, \@changes, \%flags )

Like L</set_path>, but it applies a hashref (or arrayref) of $changes to the
directory entry which exists at the named path.  Use this to update a few
attributes of a directory entry without overwriting the entire thing.

=cut

sub update_path {
	my ($self, $path, $changes, $flags)= @_;
	$flags ||= {};
	my $nodes= $self->_resolve_path($path, { follow_symlinks => 1, partial => 1, %$flags });
	croak $nodes unless ref $nodes;

	# update the final entry, after applying defaults
	my $entref= \$nodes->[-1]{entry};
	$$entref= $$entref->clone(
		defined $$entref->type? () : ( type => 'file' ),
		ref $changes eq 'HASH'? %$changes
			: ref $changes eq 'ARRAY'? @$changes
			: croak 'parameter "changes" must be a hashref or arrayref'
	);

	$self->_apply_overrides($nodes);
}

sub _apply_overrides {
	my ($self, $nodes)= @_;
	# Ensure that each node is connected to the previous via 'subtree'.
	# When we find the first connected node, we assume the rest are connected.
	my $i;
	for ($i= $#$nodes; $i > 0; $i--) {
		my $key= $self->case_insensitive? uc $nodes->[$i]{entry}->name : $nodes->[$i]{entry}->name;
		my $childref= \$nodes->[$i-1]{subtree}{$key};
		last if $$childref and $$childref eq $nodes->[$i];
		$$childref= $nodes->[$i];
	}
	# Finally, make sure the root override is set
	$self->{_path_overrides}= $nodes->[0]
		unless $i;
	1;
}

=head2 mkdir

  $fs->mkdir( \@path )

Convenience method to create an empty directory at C<path>.

=cut

sub mkdir {
	my ($self, $path)= @_;
	$self->set_path($path, { type => 'dir', ref => $self->hash_of_empty_dir });
}

=head2 touch

  $fs->touch( \@path )

Convenience method to update the timestamp of the directory entry at C<path>,
possibly creating it (as an empty file)

=cut

sub touch {
	my ($self, $path)= @_;
	$self->update_path($path, { mtime => time() });
}

=head2 unlink

  $fs->unlink( \@path )

Convenience method to remove the directory entry at C<path>.

=head2 rmdir

Alias for unlink

=cut

sub unlink {
	my ($self, $path)= @_;
	$self->set_path($path, undef);
}
*rmdir = *unlink;

# TODO: write copy and move and rename

=head2 rollback

  $fs->rollback();

Revert the FS to the state of the last commit, or the initial state.

This basically just discards all the in-memory overrides created with
"apply_path" or its various convenience methods.

=cut

sub rollback {
	my $self= shift;
	$self->{_path_overrides}= undef;
	1;
}

=head2 commit

  $fs->commit();

Merge all in-memory overrides from L</apply_path> with the directories
they override to create new directories, and store those new directories
in the CAS.

After this operation, the root_entry will be changed to reflect the new
tree.

=cut

sub commit {
	my $self= shift;
	if ($self->_path_overrides) {
		my $root_node= $self->_path_overrides;
		croak "Root override must be a directory"
			unless $root_node->{entry}->type eq 'dir';
		my $hash= $self->_commit_recursive($root_node);
		$self->{root_entry}= $root_node->{entry}->clone(ref => $hash);
		$self->{_path_overrides}= undef;
	}
	1;
}

# Takes a subtree of the datastructure generated by apply_path and encodes it
# as a directory, recursively encoding any subtrees first, then returns the
# hash of that subdir.
sub _commit_recursive {
	my ($self, $node)= @_;

	my $subtree= $node->{subtree} || {};
	my @entries;

	# Walk the directory entries and filter out any that have been overridden.
	if (defined $node->{dir} || defined $node->{entry}->ref) {
		($node->{dir} ||= $self->get_dir($node->{entry}->ref))
			or croak 'Failed to open directory "'.$node->{entry}->name.' ('.$node->{entry}->ref.')"';
		
		my ($iter, $ent);
		for ($iter= $node->{dir}->iterator; defined ($ent= $iter->()); ) {
			my $key= $self->case_insensitive? uc $ent->name : $ent->name;
			push @entries, $ent
				unless defined $subtree->{$key};
		}
	}

	# Now append the modified entries.
	# Skip the "0"s, which represent files to unlink.
	for (grep { ref $_ } values %$subtree) {
		# Check if node is a dir and needs committed
		if ($_->{subtree} and $_->{entry}->type eq 'dir') {
			my $hash= $self->_commit_recursive($_);
			$_->{entry}= $_->{entry}->clone( ref => $hash );
			delete $_->{subtree};
			delete $_->{dir};
		}
		
		push @entries, $_->{entry};
	}
	# Now re-encode the directory, using the same type as orig_dir
	return $self->hash_of_empty_dir
		unless @entries;
	my $format= $node->{dir}->format
		if $node->{dir};
	$format= 'universal' unless defined $format;
	return DataStore::CAS::FS::DirCodec->put($self->store, $format, \@entries, {});
}

package DataStore::CAS::FS::Path;
use strict;
use warnings;
use Carp;

=head1 PATH OBJECTS

=head2 path_names

Arrayref of path parts

=head2 path_ents

Arrayref of L<DataStore::CAS::FS::DirEnt|DirEnt> objects resolved from the
C<path_names>.  Lazy-built, so it might C<die> when accessed.

=head2 filesystem

Reference to the DataStore::CAS::FS it was created by.

=head2 path_name_list

Convenience list accessor for path_names arrayref

=head2 path_ent_list

Convenience list accessor for path_ents arrayref

=head2 final_ent

Convenience accessor for final element of path_ents

=head2 type

Convenience accessor for the C<type> field of the final element of C<path_ents>

=cut

# main attributes
sub path_names     { $_[0]{path_names} }
sub path_ents      { $_[0]{path_ents} || $_[0]->resolve }
sub filesystem     { $_[0]{filesystem} }

# convenience accessors
sub path_name_list { @{$_[0]->path_names} }
sub path_ent_list  { @{$_[0]->path_ents} }
sub final_ent      { $_[0]->path_ents->[-1] }
sub type           { $_[0]->final_ent->type }

=head2 resolve

  $path->resolve()

Call </resolve_path> for C<path_names>, and cache the result in the
C<path_ents> attribute.  Also returns C<path_ents>.

=head2 path

  $path->path( \@sub_path )

Get a sub-path from this path.  Returns another Path object.

=cut

# methods
sub resolve {
	$_[0]{path_ents}= $_[0]{filesystem}->resolve_path($_[0]{path_names})
}

sub path {
	my $self= shift;
	bless {
		filesystem => $self->filesystem,
		path_names => [ @{$self->path_names}, @_ ]
	}, ref($self);
}

=head2 file

  $file= $path->file();

Returns the DataStore::CAS::File of the final element of C<path_ents>,
or dies trying.

=head2 open

  $handle= $path->open

Alias for C<< $path->file->open >>

=cut

sub file {
	defined(my $hash= $_[0]->final_ent->ref)
		or croak "Path is not a file";
	$_[0]->filesystem->get($hash);
}

sub open {
	$_[0]->file->open
}

package DataStore::CAS::FS::DirCache;
use strict;
use warnings;

=head1 DIRECTORY CACHE

Directories are uniquely identified by their hash, and directory objects are
immutable.  This creates a perfect opportunity for caching recent directories
and reusing the objects.

When you call C<< $fs->get_dir($hash) >>, $fs keeps a weak reference to that
directory which will persist until the directory object is garbage collected.
It will ALSO hold a strong reference to that directory for the next N calls
to C<< $fs->get_dir($hash) >>, where the default is 64.  You can change how many
references $fs holds by setting C<< $fs->dir_cache->size(N) >>.

The directory cache is *not* global, and a fresh one is created during the
constructor of the FS, if needed.  However, many FS instances can share the
same dir_cache object, and FS methods that return a new FS instance will pass
the old dir_cache object to the new instance.

If you want to implement your own dir_cache, don't bother subclassing the
built-in one; just create an object that meets this API:

=head1 new

  $cache= $class->new( %fields )
  $cache= $class->new( \%fields )

Create a new cache object.  The only public field is C<size>.

=head1 size

Read/write accessor that returns the number of strong-references it will hold.

=head1 clear

Clear all strong references and clear the weak-reference index.

=head1 get

  $cached_dir= $cache->get( $digest_hash )

Return a cached directory, or undef if that directory has not been cached.

=head1 put

  $dir= $cache->put( $dir )

Cache the Dir object (and return it)

=cut

sub size {
	if (@_ > 1) {
		my ($self, $new_size)= @_;
		$self->{size}= $new_size;
		$self->{_recent}= [];
		$self->{_recent_idx}= 0;
	}
	$_[0]{size};
}

sub new {
	my $class= shift;
	my %p= ref($_[0])? %{$_[0]} : @_;
	$p{size} ||= 32;
	$p{_by_hash} ||= {};
	$p{_recent} ||= [];
	$p{_recent_idx} ||= 0;
	bless \%p, $class;
}

sub clear {
	$_= undef for @{$_[0]{_recent}};
	$_[0]{_by_hash}= {};
}

sub get {
	return $_[0]{_by_hash}{$_[1]};
}

sub put {
	my ($self, $dir)= @_;
	# Hold onto a strong reference for a while.
	$self->{_recent}[ $self->{_recent_idx}++ ]= $dir;
	$self->{_recent_idx}= 0 if $self->{_recent_idx} > @{$self->{_recent}};
	# Index it using a weak reference.
	Scalar::Util::weaken( $self->{_by_hash}{$dir->hash}= $dir );
	# Now, a nifty hack: we attach an object to watch for the destriction of the
	# directory.  Lazy references will get rid of the dir object, but this cleans
	# up our _by_hash index.
	$dir->{'#DataStore::CAS::FS::DirCacheCleanup'}=
		bless [ $self->{_by_hash}, $dir->hash ], 'DataStore::CAS::FS::DirCacheCleanup';
}

package DataStore::CAS::FS::DirCacheCleanup;
use strict;
use warnings;

sub DESTROY { delete $_[0][0]{$_[0][1]}; }

1;

__END__

=head1 UNICODE vs. FILENAMES

=head2 Background

Unix operates on the philosophy that filenames are just bytes.  Much of Unix
userspace operates on the philosophy that these bytes should probably be valid
UTF-8 sequences (but of course, nothing enforces that).  Other operating
systems, like modern Windows, operate on the idea that everything is Unicode
and some backward-compatible APIs exist which can represent the Unicode as
Latin1 or whatnot on a best-effort basis.  I think the "Unicode everywhere"
philosophy is arguably a better way to go, but as this tool is primarily
designed with Unix in mind, and since it is intended for saving backups of real
filesystems, it needs to be able to accurately store exactly what it find in
the filesystem.  Essentially this means it neeeds to be *able* to store
invalid UTF-8 sequences, -or- encode the octets as unicode codepoints up to
0xFF, and later know to write them out to the filesystem as octets instead
of UTF-8.

=head2 Use Cases

The primary concern is the user's experience when using this module.
While Perl has decent support for Unicode, it requires all filenames to be
strings of bytes. (i.e. strings with the unicode flag turned off)
Any time you pass a unicode string to a Perl function like open() or rename(),
perl converts it to a UTF-8 string of octets before performing the operation.
This gives you the desired result in Unix.
Unfortunately, Perl in Windows doesn't fare so well, because
it uses Windows' non-unicode API.  Reading filenames with non-latin1
characters returns garbage, and creating files with unicode strings containing
non-latin1 characters creates garbled filenames.  To properly handle unicode
outside of latin1 on Windows, you must avoid the Perl built-ins and tap
directly into the wide-character Windows API.

This creates a dilema: Should filenames be passed around the
DataStore::CAS::FS API as unicode, or octets, or some auto-detecting mix?
This dilema is further complicated because users of the library might not
have read this section of documentation, and it would be nice if The Right
Thing happened by default.

Imagine a scenario where a user has a directory named C<"\xDC"> (U with an
umlaut in latin-1) and another directory named C<"\xC3\x9C"> (U with an umlaut
in UTF-8).  "readdir" will report these as the strings I've just written, with
the unicode flag I<off>.  Modern Unix will render the first as a "?" and the
other as the U with umlaut, because it expects UTF-8 in the filesystem.

If you have the perl string "\xDC" with the UTF-8 flag off, and you try
creating that file, it will create the file names "\xDC".  However if you have
that same logical string with the UTF-8 flag on, it will become the file name
"\x3C\x9C"!

If a user is *unaware* of unicode issues, it might be better to pass around
strings of octets.  Example: the user is in "/home/\xC3\x9C", and calls "Cwd".
They get the string of octets C<"/home/\xD0">.  They then concatenate this
string with unicode C<"\x{1234}">.  Perl combines the two as
C<"/home/\x{C3}\x{9C}/\x{1234}">, however the C3 and 9C just silently went
from octets to unicode codepoints.  When the user tries opening the file, it
surprises them with "No such file or directory", because it tried opening
C<"/home/\xC3\x83\xC2\x9C/\xE1\x88\xB4">.

On Windows, perl is just generally B<broken> for high-unicode filenames.
Pure-ascii works fine, but ascii is a non-issue either way.  Those who need
unicode support will have found it from other modules, and be looking for this
section of documentation.

Interesting reading for Windows: L<http://www.perlmonks.org/?node_id=526169>

However, all this conjecture assumes a person is trying to read and write
virtual items out to their filesystem.  Since this module also provides that,
maybe people will use the ready-built implementation and this is a non-issue.

=head2 Storage Formats

The storage format is supposed to be platform-independent.  JSON seems like a
good default encoding, however it requires strings to be in Unicode.  When you
encode a mix of unicode and octet strings, Perl's unicode flag is lost and
when reading them back out you can't tell which were which.  This means that
if you take a unicode-as-octets filename and encode it with JSON and decode it
again, perl will mangle it when you attempt to open the file, and fail.  It
also means that unicode-as-octets filenames will take extra bytes to encode.

The other option is to use a plain unicode string where possible, but names
which are not valid UTF-8 are encoded as structures which can be restored
when decoding the JSON.

=head2 Conclusion

In the end, I came up with a module called L<DataStore::CAS::FS::InvalidUTF8>.
It takes a filename in native encoding, and tries to parse it as UTF-8.  If
it succeeds, it returns the string.  If it fails, it returns the string
wrapped by InvalidUTF8, with special concatenation and comparison operators.

The directory coders are written to properly save and restore these objects.

The scanner for Windows platforms will read the UTF-16 from the Windows API,
and convert it to UTF-8 to match the behavior on Unix.  The Extractor on
Windows will reverse this process.  Extracting files with invalid UTF-8 on
Windows will fail.

The default storage format uses a Unicode-only format, and a special notation
to represent strings which are not unicode (See
L<DataStore::CAS::FS::InvalidUTF8/TO_JSON|TO_JSON in InvalidUtf8>.
Other formats (Minimal and Unix) always store octets, and then re-detect UTF-8
when decoding the directory.

=head1 SEE ALSO

C<Brackup> - A similar-minded backup utility written in Perl, but without the
separation between library and application and with limited FUSE performance.

L<http://git-scm.com> - The world-famous version control tool

L<http://www.fossil-scm.org> - A similar but lesser known version control tool

L<https://github.com/apenwarr/bup> - A fantastic idea for a backup tool, which
operates on top of git packfiles, but has some glaring misfeatures that make it
unsuitable for general purpose use.  (doesn't save metadata?  no way to purge
old backups??)

L<http://rdiff-backup.nongnu.org/> - A popular incremental backup tool that
works great on the small scale but fails badly at large-scale production usage.
(exit 0 sometimes even when the backup fails? chance of leaving the backup in
a permanently broken state if interrupted? record deleted files... with files,
causing spool directory backups to contain 600,000 files in one directory?
nothing to optimize the case where a user renames a dir with 20GB of data in
it?)

=for Pod::Coverage BUILD BUILDARGS
=cut
