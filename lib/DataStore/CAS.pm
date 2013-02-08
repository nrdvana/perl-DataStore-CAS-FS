package DataStore::CAS;
use 5.008;
use strict;
use warnings;
use Carp;
use Try::Tiny;
require Scalar::Util;
require Symbol;

=head1 NAME

DataStore::CAS - Abstract base class for Content Addressable Storage

=head1 DESCRIPTION

This module lays out a very straightforward API for Content Addressable
Storage.

Content Addressable Storage is a concept where a file is identified by a
one-way message digest checksum of its content.  (usually called a "hash")
With a good message digest algorithm, one checksum will statistically only
ever refer to one file, even though the permutations of the checksum are
tiny compared to all the permutations of bytes that they can represent.

Perl uses the term 'hash' to refer to a mapping of key/value pairs, which
creates a little confusion.  The documentation of this and related modules
therefore tries to use the phrase "digest hash" to clarify when we are
referring to the output of a digest function vs. a perl key-value mapping.

=head1 PURPOSE

In short, a CAS is a key/value mapping where small-ish keys are determined
from large-ish data but no two pieces of data will ever end up with the same
key, thanks to astronomical probabilities.  You can then use the small-ish
key as a reference to the large chunk of data, as a sort of compression
technique.

One great use for CAS is finding and merging duplicated content.  If you
take two identical files (which you didn't know were identical) and put them
both into a CAS, you will get back the same hash, telling you that they are
the same.  Also, the file will only be stored once, saving disk space.

Another great use for CAS is the ability for remote systems to compare an
inventory of files and see which ones are absent on the other system.
This has applications in backups and content distribution.

=head1 SYNOPSIS

  # Create a new CAS which stores everything in plain files.
  my $cas= DataStore::CAS::Simple->new(
    path   => './foo/bar',
    create => 1,
    digest => 'SHA-256',
  );
  
  # Store content, and get its hash code
  my $hash= $cas->put_scalar("Blah");
  
  # Retrieve a reference to that content
  my $file= $cas->get($hash);
  
  # Inspect the file's attributes
  $file->size < 1024*1024 or die "Use a smaller file";
  
  # Open a handle to that file (possibly returning a virtual file handle)
  my $handle= $file->open;
  my @lines= <$handle>;

=head1 ATTRIBUTES

=head2 digest

Read-only.  The name of the digest algorithm being used.

Subclasses must set this during their constructor.

=head2 hash_of_null

The digest hash of the empty string.  The cached result of

  $cas->put('', { dry_run => 1 })

=cut

sub digest { $_[0]{digest} }

sub hash_of_null {
	my $self= shift;
	$self->{hash_of_null}= $self->put('', { dry_run => 1 }) unless defined $self->{hash_of_null};
	$self->{hash_of_null};
}

=head1 METHODS

=head2 new( %params | \%params | ?? )

Convenience method for calling the inner constructor.  Takes any variety
of parameters that makes sense to the module, and passes them to the _ctor()
method as a sensible modifyable hashref.

Note that if the user passes a hashref to new(), that hashref should be
cloned so that modifications by _ctor() are not seen by the user.

Standard parameters:

=over 10

=item digest

For storage engines which can use a pluggable digest algorithm, this
is the name of the algorithm, OR the name of a module implementing
the digest API.  (This is typically passed to the Digest module's
constructor).

=back

=cut

sub new {
	my $class= shift;
	my %params= (scalar(@_) == 1 && ref($_[0]))? %{$_[0]} : @_;
	$class->_ctor(\%params);
}

=head2 _ctor_params()

Returns a *list* of parameters (hash keys) which the constructor accepts.
Every subclass must implement this.

=head2 _ctor( \%params )

The internal constructor.  Takes exactly one parameter which must be a
modifyable hashref whose keys are in the set of _ctor_params.

=cut

our @_ctor_params= qw: digest :;
sub _ctor_params { @_ctor_params; }
sub _ctor {
	my ($class, $params)= @_;
	my %p= map { $_ => delete $params->{$_} } @_ctor_params;
	
	# Check for invalid params
	croak "Invalid parameter(s): ".join(', ', keys %$params)
		if (keys %$params);
	
	return bless \%p, $class;
}

=head2 get( $digest_hash )

Returns a DataStore::CAS::File object for the given hash, if the hash
exists in storage. Else, returns undef.

This method is pure-virtual and must be implemented in the subclass.

=cut

#sub get

=head2 put( $thing, [ \%flags ])

Convenience method.
Inspects $thing and passes it off to a more specific method.  If you want
more control over which method is called, call it directly.

=over 2

=item *

Scalars are passed to 'put_scalar'.

=item *

Instances of DataStore::CAS::File or Path::Class::File are passed to 'put_file'.

=item *

Globrefs or instances of IO::Handle are passed to 'put_handle'

=item *

Dies if it encounters anything else.

=back

The return value is the digest's hash of the stored data.

The optional 'flags' hashref can contain a wide variety of parameters.
One parameter supported by all CAS modules is the "dry_run" flag.
Setting "dry_run" to true will calculate the hash of the $thing,
but not store it.  Another parameter supported by all subclasses is
'stats'.  Setting flags->{stats} to a hashref will instruct the CAS
implementation to return information about the operation, such as number
of bytes written, compression strategies used, etc.  The statistics are
returned within that supplied hashref.  Values in the hashref are amended
or added to, so you may use the same stats hashref for multiple calls and
then see the summary for all operations when you are done.

Example:

  my $stats= {};
  $cas->put("abcdef", { stats => $stats });
  $cas->put(\*STDIN, { stats => $stats });
  $cas->put("42" x 42, { stats => $stats });
  use Data::Printer;
  p $stats;

=cut

sub put {
	goto $_[0]->can('put_scalar') unless ref $_[1];
	goto $_[0]->can('put_file')   if ref($_[1])->isa('DataStore::CAS::File') or ref($_[1])->isa('Path::Class::File');
	goto $_[0]->can('put_handle') if ref($_[1])->isa('IO::Handle') or (reftype($_[1]) eq 'GLOB');
	croak("Can't 'put' object of type ".ref($_[1]));
}

=head2 put_scalar( $scalar [, \%flags ])

Puts the literal string "$scalar" into the CAS.
If scalar is a unicode string, it is first converted to an array of UTF-8
bytes. Beware that when you next call 'get', reading from the filehandle
will give you bytes and not the original Unicode scalar.

Returns the digest's hash of the array of bytes.

See '->put' for the discussion of 'flags'.

=cut

sub put_scalar {
	my ($self, $scalar, $flags)= @_;

	# Force to plain string
	$scalar= "$scalar" if ref $scalar;

	# Convert to octets.  Actually, opening a stream to a unicode scalar gives the
	#  same result, but best to be explicit about what we want and not rely on
	#  undocumented behavior.
	utf8::encode($scalar) if utf8::is_utf8($scalar);

	open(my $fh, '<', \$scalar)
		or croak "Failed to open memory stream: $!\n";
	$self->put_handle($fh, $flags);
}

=head2 put_file( $filename | Path::Class::File | DataStore::CAS::File [, \%flags ])

Insert a file from the filesystem, or from another CAS instance.
Default implementation simply opens the named file, and passes it to
put_handle.

Returns the digest's hash of the data stored.

See '->put' for the discussion of 'flags'.

Note that passing DataStore::CAS::File objects can sometimes re-use the disk
storage of the file, and re-uses the supplied hash if the digest algorithm
matches, resulting in a significant performance boost.  In particular, copying
from one instance of DataStore::CAS::Simple to another will simply hard-link
the source to the destination.  Other engines have been similarly optimized.

=cut

sub put_file {
	my ($self, $fname, $flags)= @_;
	my $fh;
	if (ref($fname) && $fname->can('open')) {
		$fh= $fname->open
			or croak "Can't open '$fname': $!";
		binmode $fh, ':raw';
	}
	else {
		open(my $fh, '<:raw', "$fname")
			or croak "Can't open '$fname': $!";
	}
	$self->put_handle($fh, $flags);
}

=head2 put_handle( \*HANDLE | IO::Handle, [ \%flags ])

Pure virtual method.  Must be implemented by all subclasses.

Reads from $io_handle and stores into the CAS.  Calculates the digest hash
of the data as it goes.  Dies on any I/O errors.

Returns the calculated hash when complete.

If the string already exists in the CAS, most back-ends will be smart enough
to not store anything, and just return the hash.

See '->put' for the discussion of 'flags'.

=cut

# put_handle

=head2 new_write_handle

=head2 validate( $digest_hash [, %flags ])

Validate an entry of the CAS.  This is used to detect whether the storage
has become corrupt.  Returns 1 if the hash checks out ok, and returns 0 if
it fails, and returns undef if the hash doesn't exist.

Like the 'put' method, you can pass a hashref in $flags{stats} which
will receive information about the file.  This can be used to implement
mark/sweep algorithms for cleaning out the CAS by asking the CAS for all
other digest_hashes referenced by $digest_hash.

The default implementation simply reads the file and re-calculates its hash,
which should be optimized by subclasses if possible.

=cut

sub validate {
	my ($self, $hash, $flags)= @_;

	my $file= $self->get($hash);
	return undef unless defined $file;

	# Exceptions during 'put' will most likely come from reading $file,
	# which means that validation fails, and we return false.
	my $new_hash;
	try {
		# We don't pass flags directly through to get/put, because flags for validate
		#  are not the same as flags for get or put.  But, 'stats' is a standard thing.
		my %args= ( dry_run => 1 );
		$args{stats}= $flags->{stats} if $flags->{stats};
		$new_hash= $self->put_handle($file, \%args);
	}
	catch {
	};
	return (defined $new_hash and $new_hash eq $hash)? 1 : 0;
}

=head2 delete( $digest_hash [, %flags ])

DO NOT USE THIS METHOD UNLESS YOU UNDERSTAND THE CONSEQUENCES

This method is supplied for completeness... however it is not appropriate
to use in many scenarios.  Some storage engines may use referencing, where
one file is stored as a diff against another file, or one file is composed
of references to others.  It can be difficult to determine whether a given
digest_hash is truly no longer used.

The safest way to clean up a CAS is to create a second CAS and migrate the
items you want to keep from the first to the second; then delete the
original CAS.  See the documentation on the storage engine you are using
to see if it supports an efficient way to do this.  For instance,
DataStore::CAS::Simple can use hard-links on supporting filesystems,
resulting in a very efficient copy operation.

If no efficient mechanisms are available, then you might need to write a
mark/sweep algorithm and then make use of 'delete'.

Returns true if the item was actually deleted.

No flags are yet implemented, though $flags{stats} will be supported.

=cut

# sub delete

=head2 iterator([ \%flags ])

Iterate the contents of the CAS.  Returns a java-style iterator with two
methods: 'next' and 'eof'.  
('eof' seemed more appropriate to Perl than 'hasNext')
Each call to 'next' returns a digest hash string, which you can use to
'get' a file object.

Flags:

The only flag defined so far is 'prefix'.  You can use this to imitate
Git's feature of identifying an object by a portion of its hash instead
of having to paste the whole thing.  You will probably need more digits
though, because you're searching the whole CAS, and not just commit
entries.

=cut

sub iterator {
	die "TODO: Implement me"
}

=head2 file_open( $file [, \%flags ])

Open the File object (returned by 'get') and return a readable and seekable
filehandle to it.  The filehandle might be a perl filehandle, or might be a
tied object implementing the filehandle operations.

Flags:

=over 10

=item layer

Specify a perl I/O layer, like 'raw' or 'utf8'.  This is equivalent to calling
'binmode' with that argument on the filehandle.  Note that returned handles
are 'raw' by default.

=back

=cut

sub _file_destroy {}

sub _handle_destroy {}

=head1 DataStore::CAS::File Wrappers

The 'get' method returns objects of type DataStore::CAS::File. (or a subclass)

These are bare minimal wrappers that essentially just curry a few parameters
to later calls to 'open' (or possibly 'put').

The file objects returned by a store implementation may vary, but they will
always have the following API available:

=over 8

=item store

Read-only attribute; Reference to the store which created this file.

=item hash

Read-only attribute; The digest hash of the bytes of this file.

=item size

Read-only attribute; The length of the file, in bytes.

=item open([ $layer_name | %flags | \%flags ])

A convenience method to call '$file->store->file_open($file, \%flags)'

=back

Other methods may exist for the storage engine you are using; see the
documentation for your particular store.

=cut

BEGIN { $INC{'DataStore/CAS/File.pm'}= 1; }
package DataStore::CAS::File;
use strict;
use warnings;

sub store { $_[0]{store} }
sub hash  { $_[0]{hash} }
sub size  { $_[0]{size} }

sub open {
	my $self= shift;
	return $self->{store}->file_open($self)
		if @_ == 0;
	return $self->{store}->file_open($self, { @_ })
		if @_ > 1;
	return $self->{store}->file_open($self, { layer => $_[0] })
		if @_ == 1 and !ref $_[0];
	Carp::croak "Wrong arguments to 'open'";
};

sub DESTROY {
	$_[0]{store}->_file_destroy(@_);
}

our $AUTOLOAD;
sub AUTOLOAD {
	my $attr= substr($AUTOLOAD, rindex($AUTOLOAD, ':')+1);
	return $_[0]{$attr} if exists $_[0]{$attr};
	unshift @_, $_[0]{store};
	goto (
		$_[0]->can("_file_$attr")
		or Carp::croak "Can't locate object method \"_file_$attr\" via package \"".ref($_[0]).'"'
	);
}

BEGIN { $INC{'DataStore/CAS/Handle.pm'}= 1; }
package DataStore::CAS::Handle;
use strict;
use warnings;

sub new {
	my ($class, $cas, $value)= @_;
	my $glob= bless Symbol::gensym(), $class;
	${*$glob}= $cas;
	%{*$glob}= %{$value||{}};
	tie *$glob, "${class}::Tied", $glob;
	$glob;
}

sub _store { ${*${$_[0]}} }  # the scalar field of the symbol points to the CAS object
sub _data  { \%{*${$_[0]}} } # the hashref field of the symbol holds the fields of the handle

sub close    { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_close') }
sub eof      { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_eof') }
sub seek     { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_seek') }
sub tell     { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_tell') }
sub read     { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_read') }
sub readline { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_readline') }
sub print    { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_print') }
sub write    { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_write') }
sub DESTROY  { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_destroy') }

our $AUTOLOAD;
sub AUTOLOAD {
	unshift @_, ${*${$_[0]}}; # my ($cas, $value)
	my $attr= substr($AUTOLOAD, rindex($AUTOLOAD, ':')+1);
	goto (
		$_[0]->can("_handle_$attr")
		or Carp::croak "Can't locate object method \"_handle_$attr\" via package \"".ref($_[0]).'"'
	);
}

#
# The following are some default implementations to make subclassing less cumbersome.
#

# virtual handles are unlikely to have one
sub fileno { -1 }

# I'm not sure why anyone would ever want this function, but I'm adding
#  it for completeness.
sub getc {
	my $c;
	$_[0]->read($c, 1)? $c : undef;
}

# same API...
*sysread= *read;
*syswrite= *write;
*sysseek= *seek;

# as if anyone would want to write their own printf implementation...
sub printf {
	@_= ($_[0], sprintf($_[1], $_[2..$#_]));
	goto $_[0]->can('print');
}

package DataStore::CAS::Handle::Tied;
use strict;
use warnings;

sub TIEHANDLE {
	my ($class, $ref)= @_;
	Scalar::Util::weaken($ref);
	bless \$ref, $class;
}

sub WRITE    { unshift @_, ${(shift)}; goto $_[0]->can('write') }
sub PRINT    { unshift @_, ${(shift)}; goto $_[0]->can('print') }
sub PRINTF   { unshift @_, ${(shift)}; goto $_[0]->can('printf') }
sub READ     { unshift @_, ${(shift)}; goto $_[0]->can('read') }
sub READLINE { unshift @_, ${(shift)}; goto $_[0]->can('readline') }
sub GETC     { unshift @_, ${(shift)}; goto $_[0]->can('getc') }
sub EOF      { unshift @_, ${(shift)}; goto $_[0]->can('eof') }
sub CLOSE    { unshift @_, ${(shift)}; goto $_[0]->can('close') }
sub FILENO   { unshift @_, ${(shift)}; goto $_[0]->can('fileno') }
sub SEEK     { unshift @_, ${(shift)}; goto $_[0]->can('seek') }
sub TELL     { unshift @_, ${(shift)}; goto $_[0]->can('tell') }
# DESTROY is handled by the encapsulating blessed globref.

1;
