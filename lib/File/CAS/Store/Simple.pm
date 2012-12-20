package File::CAS::Store::Simple;

use 5.006;
use strict;
use warnings;

use parent 'File::CAS::Store';

=head1 NAME

File::CAS::Store::Simple - Simple file/directory based CAS implementation

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

    use File::CAS::Store::Simple;

    my $sto= File::CAS::Store::Simple->new(path => './foo');
	
    my $info= $sto->get('93785638AHAD8823648346');
	print $info? $info->{size} : '<no such hash>';
	
	my $hash= $sto->put('Long string of text');
	
	open FH, "<:raw", $fileName or die "$!";
	my $hash= $sto->put(\*FH);
	
	my $buf;
	my $file= $sto->get($hash);
	
	# This breaks the file object's read/write methods, but
	#  this bypasses the buffering of the File object for a
	#  tiny speed boost.  It uses sysread, which also
	#  bypasses Perls's buffering.
	$got= $sto->readFile($file, $buf, 1024);
	
=cut

use Carp;
use Try::Tiny;
use IO::File;
use Digest;
use Cwd ();
use File::Spec::Functions 'catfile', 'catdir';
use File::Copy;
use File::Temp 'tempfile';
use File::Path 'make_path';
use File::CAS::File;

=head1 ATTRIBUTES

=head2 path

Read-only.  The filesystem path where the store is rooted.

The root of the store will always have a file named 'file_cas_store_simple.yml'
and will contain a hash entry for the empty string, and a bunch of directories
for each hash prefix: '00' .. 'FF'.

=head2 copyBufferSize

Number of bytes to copy at a time when saving data from a filehandle to the
CAS.  This is a performance hint, and the default is usually fine.

=head2 digest

Read-only.  Algorithm used to calculate the hash values.  Default is 'sha256'.
Valid values are anything that perl's Digest module accepts, though this could
be extended in the future.

This value cannot be changed on the fly; custom values must be passed to the
constructor.

If you specify the value of 'auto', the constructor will set 'digest' to match
the store's configuration.  If the store has not been created yet, the
constructor will choose an algorithm of 'sha1'.

=head2 hashOfNull

Read-only.  This is a simple cache of the hash value that the chosen hash
algorithm gives for an empty string.  You can compare a hash to this value to
find out whether it references an empty file.

=head2 _metaFile

Intended mainly for internal use, this is the filename of the store's metadata
file, most likely encoded as YAML.

=cut

sub path           { $_[0]{path} }
sub pathReal       { $_[0]{pathReal} }
sub digest         { $_[0]{digest} }

sub copyBufferSize { $_[0]{copyBufferSize}= $_[1] if (@_ > 1); $_[0]{copyBufferSize} || 256*1024 }
sub hashOfNull     { $_[0]{hashOfNull} }

=head1 METHODS

=head2 new( \%params | %params )

Constructor.  It will load (and possibly create) a CAS Store.

'path' points to the cas directory.  Trailing slashes don't matter.
If 'path' is relative, it will be resolved during the constructor to an
absolute real path.  (the 'path' attribute remains the same, in case you
want to reference it later, but it cannot be changed)  The 'pathReal'
attribute shows the resolved path.

'pathBase' overrides the "current directory" for resolving a relative 'path'.
We want to support relative paths, but the current directory is essentially
a global variable, which can be inconvenient.
(While we could require the user to resolve relative paths first, and keep
that detail out of this module, they might later want to know whether this
object was initialized from a relative or absolute path, so we allow it.)

'copyBufferSize' initializes the respective attribute.

'digest' gets loaded from the store's configuration, and may not be passed to
the constrctor.  For creating a new store with a specific digest, specify
'defaultDigest'.

If 'create' is specified, and 'path' refers to an empty directory, a fresh store
will be initialized.

'ignoreVersion' allows you to load a Store even if it was created with a newer
version of the Store::Simple package that you are now using.  (or a different
package entirely)

To dynamically find out which parameters the constructor accepts,
call $class->_ctor_params(), which returns a list of valid keys.

=cut

# We inherit 'new', and implement '_ctor'.  The parameters to _ctor are always a hash.

our @_ctor_params= qw: path copyBufferSize create ignoreVersion pathBase defaultDigest :;
sub _ctor_params { @_ctor_params; }
sub _ctor {
	my ($class, $params)= @_;
	my %p= map { $_ => delete $params->{$_} } @_ctor_params;
	
	# Check for invalid params
	croak "Invalid parameter: ".join(', ', keys %$params)
		if (keys %$params);
	
	# extract constructor flags which don't belong in attributes
	my $create= delete $p{create};
	my $ignoreVersion= delete $p{ignoreVersion};
	my $pathBase= delete $p{pathBase};
	my $defaultDigest= delete $p{defaultDigest} || 'SHA-1';
	
	# Calculate the absolute real path
	defined $p{path} or $p{path}= '.';
	defined $pathBase or $pathBase= Cwd::getcwd();
	$p{pathReal}= Cwd::realpath( File::Spec->rel2abs( $p{path}, $pathBase ) );
	
	# Check directory
	unless (-f catfile($p{pathReal}, 'conf', 'VERSION')
		and -f catfile($p{pathReal}, 'conf', 'DIGEST') )
	{
		croak "Path does not appear to be a valid CAS : '$p{pathReal}'"
			unless $create;
		
		# Here, we are creating a new CAS directory
		
		my $self= bless { %p, digest => $defaultDigest }, $class;
		$self->_initializeStore();
		
		# We could just use that '$self', but we want to double-check our initialization
		#  by continuing through the regular constructor code path.
	}
	
	my $self= bless \%p, $class;
	
	$self->_loadDigest();

	try {
		$self->_checkVersion();
	}
	catch {
		$ignoreVersion ? warn($_) : die($_);
	};
	
	# Properly initialized CAS will always contain an entry for the empty string
	$self->{hashOfNull}= $self->_newHash->hexdigest();
	croak "CAS dir '".$self->pathReal."' is missing a required file (has it been initialized?)"
		unless $self->get($self->hashOfNull);
	
	return $self;
}

# Called during constrctor when creating a new Store directory.
sub _initializeStore {
	my ($self)= @_;
	make_path(catdir($self->pathReal, 'conf'));
	$self->_writeConfig('VERSION', ref($self).' '.$VERSION."\n");
	$self->_writeConfig('DIGEST', $self->digest."\n");
	$self->put('');
}

# In the name of being "Simple", I decided to just read and write
# raw files for each parameter instead of using JSON or YAML.
# It is not expected that this module will have very many options.
# Subclasses will likely use YAML.

sub _writeConfig {
	my ($self, $fname, $content)= @_;
	my $path= catfile($self->pathReal, 'conf', $fname);
	my $f= IO::File->new($path, '>')
		or die "Failed to open '$path' for writing: $!\n";
	$f->print($content) && $f->close()
		or die "Failed while writing '$path': $!\n";
}
sub _readConfig {
	my ($self, $fname)= @_;
	my $path= catfile($self->pathReal, 'conf', $fname);
	open(my $f, '<', $path)
		or die "Failed to read '$path' : $!\n";
	local $/= undef;
	return <$f>;
}

# This method loads the digest configuration and validates it
# It is called during the constructor.
sub _loadDigest {
	my $self= shift;
	
	# Get the digest algorithm name
	chomp( $self->{digest}= $self->_readConfig('DIGEST') );
	
	# Check for digest algorithm availability
	my $found= ( try { $self->_newHash; 1; } catch { 0; } )
		or die "Digest algorithm '".$self->digest."' is not available on this system.\n";
}

# This method loads the version the store was initialized with
#  and checks to see if we are compatible with it.
sub _checkVersion {
	my $self= shift;

	# Version str is "$PACKAGE $VERSION\n", where version is a number but might have a string suffix on it
	my $version_str= $self->_readConfig('VERSION');
	($version_str =~ /^([A-Za-z0-9:_]+) ([0-9.]+)/)
		or die "Invalid version string in storage dir '".$self->pathReal."'\n";

	# Check $PACKAGE
	($1 eq ref($self))
		or die "Class mismatch: storage dir was created with $1 but you're trying to access it with ".ref($self)."\n";

	# Check $VERSION
	($2 > 0 and $2 <= $VERSION)
		or die "Storage dir '".$self->pathReal."' was created by version $2 of ".ref($self).", but this is only $VERSION\n";
}

=head2 getConfig

This method returns a hash which can be used as the 'cas' parameter to
File::CAS's constructor to re-create this object.

=cut

sub getConfig {
	my $self= shift;
	return {
		CLASS => ref $self,
		VERSION => $VERSION,
		digest => $self->digest,
		path => $self->path,
		(defined $self->{copyBufferSize}? ( copyBufferSize => $self->copybufferSize ) : ()),
	};
}

=head2 get

This does NOT return the data bytes associated with the hash.

It returns a File::CAS::File object which you can call methods on
to read the data from your file.  You can also ask it for a virtual (tied)
filehandle which can be passed to perl functions which expect a filehandle.
This allows us to work with files larger than memory.

=cut
sub get {
	my $fname= catfile($_[0]->_pathForHash($_[1]));
	return undef
		unless (my ($sz, $blksize)= (stat $fname)[7,11]);
	return File::CAS::File->_ctor({
		# required
		store     => $_[0],
		hash      => $_[1],
		size      => $sz,
		# extra info
		_blockSize => $blksize,
		_storeFile => $fname,
	});
}

=head2 $hash= put( $scalar_or_handle [,$hash] )

Store the scalar data, or the data form the given handle.

If $hash is specified, this module trusts the caller that the
hash is known, and uses that one instead of recalculating it.
Use this optimization **carefully**, as incorrect hash values
can potentially corrupt more data than just this one file!

Note that instances of Path::Class::* are handled by the
File::CAS module, not the store.

=cut
sub put {
	my ($self, $data, $hash)= @_;
	my ($destFh, $fname, $dest);
	if (defined $hash) {
		my ($dir, $file)= $self->_pathForHash($hash);
		$dest= catfile($dir, $file);
		return $hash if -f $dest; # don't save the data again if the hash already exists
		make_path($dir);
		($destFh, $fname)= tempfile( 'temp-XXXXXXXX', DIR => $dir );
	} else {
		($destFh, $fname)= tempfile( 'temp-XXXXXXXX', DIR => $self->pathReal );
	}
	binmode $destFh;
	
	try {
		my $digest= $self->_newHash unless defined $hash;
		# simple scalar
		if (!ref $data) {
			$digest->add($data) unless defined $hash;
			_writeAllOrDie($destFh, $data);
		}
		# else we read from the supplied file handle
		else {
			my $buf;
			while(1) {
				my $got= sysread($data, $buf, $self->copyBufferSize);
				if ($got) {
					# hash it (maybe)
					$digest->add($buf) unless defined $hash;
					# then write to temp file
					_writeAllOrDie($destFh, $buf);
				} elsif (!defined $got) {
					next if ($!{EINTR} || $!{EAGAIN});
					croak "while reading input: $!";
				} else {
					last;
				}
			}
		}
		close $destFh
			or croak "while saving copy: $!";
		unless (defined $hash) {
			$hash= $digest->hexdigest;
			my ($dir, $file)= $self->_pathForHash($hash);
			make_path($dir);
			$dest= catfile($dir, $file);
		}
		
		if (-f $dest) {
			# we already have it
			unlink $fname;
		} else {
			# move it into place
			move($fname, $dest)
				or croak "$!";
		}
	} catch {
		my $msg= "$_";
		close $destFh;
		unlink $fname;
		die "$msg\n";
	};
	$hash;
}

=head2 caclHash( $scalar_or_handle )

Calculate the hash of a scalar or the contents of a filehandle
without storing it.

Returns the alphanumeric hash.

=cut
sub calcHash {
	my ($self, $data)= @_;
	my $digest= $self->_newHash;
	$digest->add($data) unless ref $data;
	$digest->addfile($data) if ref $data;
	$digest->hexdigest;
}

=head2 validate( $hash )

Verify that a hash references data that actually adds up to that hash.

If $hash is undef, this will validate every hash in the store.

=cut
sub validate {
	my ($self, $hash)= @_;
	if ($hash) {
		my $info= $self->get($hash) or croak "No entry for hash '$hash'";
		open my $fh, '<:raw', $info->{_storeFile};
		my $actual= $self->calcHash($fh);
		return $hash eq $actual;
	} else {
		croak "Unimplemented";
	}
}

=head2 readFile( $lookupInfo, $buffer, $length [, $offset] )

Same API, symantics, and caveats as read, with the exception
that additional error codes (from 'open') might also show up,
due to implementation.

=cut
sub readFile {
	restart:
	unless (defined $_[1]{_fh}) {
		open( $_[1]{_fh}, '<:raw', $_[1]{_storeFile} )
			or return undef;
	}
	my $got= sysread($_[1]{_fh}, $_[2], $_[3], $_[4] || 0);
	goto restart if !defined($got) && $!{EINTR};
	$got;
}

=head2 seekFile( $lookupInfo, $position, $whence )

Same API as sysseek.

=cut

sub seekFile {
	unless (defined $_[1]{_fh}) {
		open( $_[1]{_fh}, '<:raw', $_[1]{_storeFile} )
			or return undef;
	}
	sysseek($_[1]{_fh}, $_[2], $_[3]);
}

=head2 closeFile( $lookupInfo )

You may call this if you wish to free any filesystem resources used by
previous calls to readFile, seekFile, or tellFile, and still retain a
reference to the lookupInfo.

If the lookupInfo goes out of scope, it will be closed
automatically, which is the usual way of releasing resources.

Always returns 1.  Files are never open for writing, and whether
an underlying file was actually open is an implementation detail
that you should pay no attention to.

=cut
sub closeFile {
	if ($_[1]{fh}) {
		close($_[1]{fh});
		delete $_[1]{fh};
	}
	1;
}

sub _writeAllOrDie {
	my ($fh, $data)= @_;
	my $wrote;
	my $ofs= 0;
	while ($ofs < length($data)) {
		$wrote= syswrite($fh, $data, length($data) - $ofs, $ofs);
		if ($wrote) {
			$ofs+= $wrote;
		} else {
			croak "$!"
				unless !defined($wrote) && ($!{EINTR} || $!{EAGAIN});
		}
	}
}

sub _newHash {
	Digest->new($_[0]{digest});
}

sub _pathForHash {
	my ($self, $hash)= @_;
	catfile( $self->pathReal, substr($hash, 0, 2), substr($hash, 2, 2) ), substr($hash,4);
}

=head1 AUTHOR

Michael Conrad, C<< <mike at nrdvana.net> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-file-cas at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=File-CAS>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc File::CAS::Store::Simple


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=File-CAS>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/File-CAS>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/File-CAS>

=item * Search CPAN

L<http://search.cpan.org/dist/File-CAS/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2012 Michael Conrad.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1; # End of File::CAS::Store::Simple
