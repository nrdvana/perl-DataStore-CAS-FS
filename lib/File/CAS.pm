package File::CAS;

use 5.006;
use strict;
use warnings;

=head1 NAME

File::CAS - Content-Addressable Storage for file trees

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.0100';
sub VersionParts {
	return (int($VERSION), (int($VERSION*100)%100), (int($VERSION*10000)%100));
}

use Carp;
use File::Spec;
use File::CAS::File;
use File::CAS::Dir;
use File::CAS::Scanner;

=head1 SYNOPSIS

  # Create a new CAS backed by 'File::CAS::Store::Simple', which stores
  #   everything in plain files.
  my $cas= File::CAS->new(
    store => {
      CLASS => 'Simple',
      path => './foo/bar',
      create => 1,
      defaultDigest => 'SHA-256'
    }
  );
  
  # Store content, and gets its hash code
  my $hash= $cas->putScalar("Blah");
  
  # Create a CAS that reads an existing Store
  $cas= File::CAS->new( store => { CLASS => 'Simple', path => 'foo/bar' } );
  
  # Retrieve a file handle object to that content
  my $file= $cas->get($hash);
  
  # Read from the handle object
  my @lines= <$file>;
  
  # Recursively store directories from the real filesystem into the CAS
  # Make sure not to include the Store's path!
  my $rootHash= $cas->putDir("/home/my_user/stuff");
  
  # Store the same dir again, but only look at files whose size or
  #  timestamp have changed
  my $rootHash2= $cas->putDir("/home/my_user/stuff", $rootHash);
  
  # Fetch and decode a directory by its hash
  my $dir= $cas->getDir($rootHash);
  
  # Walk the directory to a known path
  my $stuff= $dir->subdir('home')->subdir('my_user')->subdir('stuff');
  
  # Same as above, but doesn't decode the final directory
  my $stuff= $dir->find('home/my_user/stuff');

=head1 DESCRIPTION

File::CAS is an object that implements Content Addressable Storage, and an
additional (optional) file/directory hierarchy on top of it.

Content Addressable Storage is a concept where a file is identified by a hash
of its content, and you can only retrieve it if you know the hash you are
looking for.  Two files with identical content always hash to the same value,
so you never have duplicated files in a CAS.  While it is possible for two
*non-identical* files to hash to the same value, a good hash algorithm makes
that statistically unlikely to occur before the end of the universe.
(and, File::CAS lets you pick your hash function! so you can use SHA-512 if
you're paranoid.)

File::CAS extends this to also include a directory object (File::CAS::Dir) to
let you store traditional file hierarchies in the CAS, and look up files by a
path name (so long as you know the hash of the root).
Each directory is serialized into a file which is stored in the CAS like any
other, resulting in a very clean implementation.  You cannot determine whether
a file is a directory or not without the context of the containing directory,
so File::CAS::Dir::Entry objects are used to hold metadata needed for this.
You must keep track of your root directory's hash in order to begin walking
the directory tree.  The root hash encompases all the content of the entire
tree, so the root hash will change each time you alter a directory, while any
unchanged files in that tree will be re-used.  You can see great applications
of this design in a number of version control systems, notably Git.

File::CAS is mostly a wrapper around pluggable modules that handle the details.
The primary object involved is a File::CAS::Store, which performs the hashing
and storage actions.  There is also File::CAS::Scanner for scanning the real
filesystem to import directories, and various directory encoding classes like
File::CAS::Dir::Unix used to serialize and deserialize the directories in an
efficient manner for your system.

=head1 ATTRIBUTES

=head2 store - read-only

An instance of 'File::CAS::Store' or a subclass.

=head2 scanner - read/write

An instance of File::CAS::Scanner, or subclass.  It is responsible for
scanning real filesystem directories during "putDir".  If you didn't
specify one in the constructor, one will be created automatically.

You may alter this instance, or specify an alternate one during the
constructor, or overwrite this attribute with a new object reference.

=cut

sub store { $_[0]{store} }

sub scanner { $_[0]{scanner}= $_[1] if (scalar(@_)>1); $_[0]{scanner} }

=head1 METHODS

=head2 new( %args | \%args )

Parameters:

=over

=item store - required

It may be a class name like 'Simple' which
refers to the namespace File::CAS::Store::, or it may be a fully constructed
instance.  If it is a class name, you may also specify parameters for that
class in-line with the rest of the CAS parameters, and they will be sorted
out automagically.

=item scanner - optional

Allows you to specify a scanner object
which is used during "putDir" to collect metadata about the directory
entries.

=item filter - optional

Alias for scanner->filter.

=back

=cut

sub new {
	my $class= shift;
	my %p= ref($_[0])? %{$_[0]} : @_;
	
	defined $p{store} or croak "Missing required parameter 'store'";
	
	# coercion of store parameters to Store object
	if ($p{store} && !ref $p{store}) {
		$p{store}= { CLASS => $p{store} };
	}
	if (ref $p{store} eq 'HASH') {
		my %storeParams= %{$p{store}};
		my $storeClass= delete $storeParams{CLASS} || 'File::CAS::Store::Simple';
		_requireClass($storeClass);
		$storeClass->isa('File::CAS::Store')
			or die "'$storeClass' is not a valid Store class\n";
		$p{store}= $storeClass->new(\%storeParams);
	}
	
	# coercion of scanner parameters to Scanner object
	$p{scanner} ||= { };
	if (ref $p{scanner} eq 'HASH') {
		my %scannerParams= %{$p{scanner}};
		my $scannerClass= delete $scannerParams{CLASS} || 'File::CAS::Scanner';
		_requireClass($scannerClass);
		$scannerClass->isa('File::CAS::Scanner')
			or die "'$scannerClass' is not a valid Scanner class\n";
		$p{scanner}= $scannerClass->new(\%scannerParams);
	}
	
	$class->_ctor(\%p);
}

sub _requireClass($) {
	my $pkg= shift;
	
	# We're loading user-supplied class names.  Protect against code injection.
	($pkg =~ /^[A-Za-z0-9_:]+$/)
		or croak "Invalid perl package name: '$pkg'\n";
	
	unless ($pkg->can('new')) {
		my ($fail, $err)= do {
			local $@;
			((not eval "require $pkg;"), $@);
		};
		die $err if $fail;
	}
	1;
}

sub getConfig {
	my $self= shift;
	return {
		CLASS => ref $self,
		VERSION => $VERSION,
		store => $self->store->getConfig,
		scanner => $self->scanner->getConfig,
	};
}

our @_ctor_params= qw: scanner store dirClass :;
sub _ctor_params { @_ctor_params }

sub _ctor {
	my ($class, $params)= @_;
	my $p= { map { $_ => delete $params->{$_} } @_ctor_params };
	croak "Invalid parameter: ".join(', ', keys %$params)
		if (keys %$params);
	defined $p->{store} and $p->{store}->isa('File::CAS::Store')
		or croak "Missing/invalid parameter 'store'";
	defined $p->{scanner} and $p->{scanner}->isa('File::CAS::Scanner')
		or croak "Missing/invalid parameter 'scanner'";
	$p->{_dircache}= {};
	$p->{_dircache_recent}= [];
	$p->{_dircache_recent_idx}= 0;
	$p->{_dircache_recent_mask}= 63;
	bless $p, $class;
}

=head2 get( $hash )

This passes through to File::CAS::Store, which looks up the hash and either
returns a File::CAS::File object (which you can read like a filehandle) or
undef if the content does not exist.

=cut

sub get {
	# my ($self, $hash)= @_;
	$_[0]{store}->get($_[1]);
}

=head2 getDir( $hash )

This returns a de-serialized directory object found by its hash.  It is a
shorthand for 'get' on the Store, and deserializing enough of the result to
create a usable File::CAS::Dir object (or subclass).

Also, this method caches recently used directory objects, since they are
immutable. (but woe to those who break the API and modify their directory
objects!)

(Directories in File::CAS are just files which can be decoded by
 File::CAS::Dir (or various pluggable subclasses) to produce a set of
 File::CAS::Dir::Entry objects, which reference other hashes, which might be
 files or directories or other things)

=cut

sub getDir {
	my ($self, $hash)= @_;
	my $dircache= $self->{_dircache};
	my $ret= $dircache->{$hash};
	unless (defined $ret) {
		$ret= File::CAS::Dir->new($_[0]{store}->get($_[1]));
		if ($ret) {
			Scalar::Util::weaken($dircache->{$hash}= $ret);
			# We don't want a bunch of dead keys laying around in our cache, so we use a clever trick
			# of attaching an object to the directory whose destructor removes the key from our cache.
			# We use a blessed coderef to prevent seeing circular references while debugging.
			$ret->{_dircache_cleanup}= bless sub { delete $dircache->{$hash} }, 'File::CAS::DircacheCleanup';
		}
	}
	# Hold a reference to any dir requested in the last N getDir calls.
	$self->{_dircache_recent}[$self->{_dircache_recent_idx}++ & $self->{_dircache_recent_mask}]= $ret;
	$ret;
}

package File::CAS::DircacheCleanup;

sub DESTROY {
	&{$_[0]}; # Our 'object' is actually a blessed coderef that removes us from the cache.
}

package File::CAS;

=head2 getEmptyDirHash

This returns the canonical value of an encoded empty directory. In other
words, File::CAS::Dir->SerializeEntries([],{}).  This value is cached for
performance.

=cut

sub getEmptyDirHash {
	my $self= shift;
	return $self->{emptyDirHash} ||=
		do {
			my $emptyDir= File::CAS::Dir->SerializeEntries([],{});
			$self->{store}->put($emptyDir);
		};
}

=head2 getEmptyFileHash

This returns the value you get when storing an empty string.  This value
is cached for performance.

=cut

sub getEmptyFileHash {
	my $self= shift;
	return $self->{store}->hashOfNull;
}

sub _clearDirCache {
	my ($self)= @_;
	$self->{_dircache}= {};
	$self->{_dircache_recent}= [];
	$self->{_dircache_recent_idx}= 0;
}

=head2 calcHash

Calculate the hash of something without adding it to the CAS.

=cut

sub calcHash {
	my ($self, $thing)= @_;
	if (ref($thing)) {
		if (ref($thing)->isa('Path::Class::Dir')) {
			return $self->dirScanner->calcHash($self, $thing);
		} elsif (ref($thing)->isa('Path::Class::File')) {
			open my $f, '<:raw', "$thing";
			return $self->{store}->calcHash($f);
		}
	}
	$self->{store}->calcHash($thing);
}

=head2 findHashByPrefix

Git allows you to use partial hashes so long as you specify enough of the
hash to distinctly identify it.  We allow that feature as well, though at
a slightly higher cost since we search the whole of the CAS and not just
commits.

=cut

sub findHashByPrefix {
	my ($self, $prefix)= @_;
	return $prefix if $self->get($prefix);
	warn "TODO: Implement findHashByPrefix\n";
	return undef;
}

=head2 put( $thing )

Puts an unknown thing into the CAS by testing its type
and calling an appropriate method to do so.
Returns the store's hash of that thing.

=cut

sub put {
	return $_[0]->putScalar($_[1]) unless ref $_[1];
	return $_[0]->putDir($_[1])    if ref($_[1])->isa('Path::Class::Dir');
	return $_[0]->putFile($_[1])   if ref($_[1])->isa('Path::Class::File');
	# else assume handle
	$_[0]{store}->put($_[1]);
}

=head2 putScalar( $string )

Puts a constant string of data into the CAS.
Returns the Store's hash of that string.

=cut

sub putScalar {
	my ($self, $scalar)= @_;
	$scalar= "$scalar" if ref $scalar;
	$self->{store}->put($scalar);
}

=head2 putHandle( \*FILE | IO::Handle )

Reads the file handle and writes the data into the store,
returning the hash of the overall stream upon completion.

=cut

sub putHandle {
	$_[0]{store}->put($_[1]);
}

=head2 putFile( $filename | Path::Class::File )

Copies the named file from the filesystem to the CAS,
returning the hash of the file when complete.

=cut

sub putFile {
	my ($self, $fname)= @_;
	open(my $fh, '<:raw', "$fname")
		or croak "Can't open '$fname': $!";
	$self->{store}->put($fh);
}

=head2 putDir( $dirname | Path::Class::Dir, [ $dirHint ] )

Scans the named directory, using the optional hint to optimize the scan,
and serializes it with the default directory plugin, then stores the
serialized data and returns the Store's hash of that data.

If $dirHint is given, it instructs the directory scanner to re-use the
hashes of the old files whose name, size, and date match the current state
of the directory, rather than re-hashing every single file.  This makes
scanning much faster, but removes the guarantee that you actually get a
complete backup of your files.  Also on the plus side, it reduces the wear
and tear on your harddrive. (but irrelevant for solid state drives)

$dirHint must be a File::CAS::Dir object or a hash of one that has been
stored previously.

=cut

sub putDir {
	my ($self, $dir, $dirHint)= @_;
	$self->scanner->storeDir($self, $dir, $dirHint);
}

=head2 resolvePath( $rootDirEnt, \@pathNames | $pathString, [ \$error_out ] )

Returns an arrayref of File::CAS:Dir::Entry objects corresponding to the
specified path, starting with $rootDirEnt.  This function essentially
performs the same operation as 'File::Spec::realpath', but for the virtual
filesystem, and gives you an array of directory entries instead of a string.

If the path contains symlinks or '..', they will be resolved properly.
Symbolic links are not followed if they are the final element of the path.
To force symbolic links to be resolved, simply append '' to @pathNames, or
append '/' to $pathString.

If the path does not exist, or cannot be resolved for some reason, this
method returns undef, and the error is stored in the optional parameter
$error_out.  If you would rather die on an unresolved path, use
'resolvePathOrDie()'.

If you want symbolic links to resolve properly, $rootDirEnt must be the
filesystem root directory. Passing any other directory will cause a chroot-like
effect which you may or may not want.

=head2 resolvePathOrDie

Same as resolvePath, but calls 'croak' with the error message if the
resolve fails.

=head2 resolvePathPartial

Same as resolvePath, but if the path doesn't exist, any missing directories
will be given placeholder Dir::Entry objects.  You can test whether the path
was resolved completely by checking if $result->[-1]->type is defined.

=cut

sub resolvePath {
	my ($self, $rootDirEnt, $path, $error_out)= @_;
	my $ret= $self->_resolvePath($rootDirEnt, $path, 0);
	return $ret if ref($ret) eq 'ARRAY';
	$$error_out= $ret;
	return undef;
}

sub resolvePathPartial {
	my ($self, $rootDirEnt, $path, $error_out)= @_;
	my $ret= $self->_resolvePath($rootDirEnt, $path, 1);
	return $ret if ref($ret) eq 'ARRAY';
	$$error_out= $ret;
	return undef;
}

sub resolvePathOrDie {
	my ($self, $rootDirEnt, $path)= @_;
	my $ret= $self->_resolvePath($rootDirEnt, $path, 0);
	return $ret if ref($ret) eq 'ARRAY';
	croak $ret;
}

sub _resolvePath {
	my ($self, $rootDirEnt, $path, $createMissing)= @_;

	my @subPath= ref($path)? @$path : File::Spec->splitdir($path);
	
	my @dirEnts= ( $rootDirEnt );
	die "Root directory must be a directory"
		unless $rootDirEnt->type eq 'dir';

	while (@subPath) {
		my $ent= $dirEnts[-1];
		my $dir;

		if ($ent->type eq 'symlink') {
			# Sanity check on symlink entry
			my $target= $ent->linkTarget;
			defined $target and length $target
				or return 'Invalid symbolic link "'.$ent->name.'"';
			
			# Resolve the path in the link before continuing on the remainder of the current path
			unshift @subPath, File::Spec->splitdir($target);
			pop @dirEnts;
			
			# If an absolute link, we start over from the root
			@dirEnts= ( $rootDirEnt )
				if (substr($target, 0, 1) eq '/');
			
			next;
		}

		return 'Cannot descend into directory entry "'.$ent->name.'" of type "'.$ent->type.'"'
			unless ($ent->type eq 'dir');

		# If no hash listed, directory was not stored. (i.e. --exclude option during import)
		if (defined $ent->hash) {
			$dir= $self->getDir($ent->{hash});
			defined $dir
				or return 'Failed to open directory "'.$ent->name.'"';
		}
		else {
			return 'Directory "'.File::Spec->catdir(map { $_->name } @dirEnts).'" is not present in storage'
				unless $createMissing;
		}

		# Get the next path component, ignoring empty and '.'
		my $name= shift @subPath;
		my $ent;
		next unless defined $name and length $name and ($name ne '.');

		# We handle '..' procedurally, moving up one real directory and *not* backing out of a symlink.
		# This is the same way the kernel does it, but perhaps shell behavior is preferred...
		if ($name eq '..') {
			die "Cannot access '..' at root directory"
				unless @dirEnts > 1;
			pop @dirEnts;
		}
		# If we're working on an available directory and the sub-entry exists, append it.
		elsif (defined $dir and defined ($ent= $dir->getEntry($name))) {
			push @dirEnts, $ent;
		}
		# Else it doesn't exist and we either fail or create it.
		else {
			return 'No such directory entry "'.$name.'" at "'.File::Spec->catdir(map { $_->name } @dirEnts).'"'
				unless $createMissing;

			# Here, we create a dummy entry for the $createMissing feature.
			# It is a directory if there are more path components to resolve.
			push @dirEnts, File::CAS::Dir::Entry->new(name => $name, (@subPath? (type=>'dir') : ()));
		}
	}
	
	\@dirEnts;
}

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

=cut

1;
