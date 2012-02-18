package File::CAS::Store::Simple;

use 5.006;
use strict;
use warnings;

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
	my $info= $sto->get($hash);
	my $got= $sto->read($info, $buf, 1024);
	
=cut

use Carp;
use File::Spec::Functions 'catfile', 'catdir';
use File::Copy;
use File::Temp 'tempfile';
use File::Path 'make_path';
use Digest::SHA;
use IO::File;
use YAML 'LoadFile', 'DumpFile';
use Try::Tiny;
use Params::Validate ();
use File::CAS::File;

=head1 ATTRIBUTES

=head2 path

Read-only.  The filesystem path where the store is rooted.
(the root of the store will always have a file named 'file_cas_store_simple.yml'
and will contain a hash entry for the empty string.)

=head2 copyBufferSize

Number of bytes to copy at a time when saving data from a filehandle to the
CAS.

=head2 alg

Read-only.  Algorithm used to calculate the hash values.  Default is 'sha256'.
Valid values are anything that Digest::SHA accepts, though this could be
extended in the future.

This value cannot be changed on the fly; custom values must be passed to the
constructor.

If you specify the value of 'auto', the constructor will set 'alg' to match
the store's configuration.  If the store has not been created yet, the
constructor will choose an algorithm of 'sha1'.

=head2 hashOfNull

Read-only.  This is a simple cache of the hash value that the chosen hash
algorithm gives for an empty string.  You can compare a hash to this value to
find out whether it references an empty file.

=head2 _infoFile

Intended mainly for internal use, this is the filename of the store's metadata
file, most likely encoded as YAML.

=cut

sub path           { $_[0]{path} }
sub copyBufferSize { (@_ > 1)? $_[0]{copyBufferSize}= $_[1] : $_[0]{copyBufferSize} }
sub alg            { $_[0]{alg} }
sub hashOfNull     { $_[0]{hashOfNull} }
sub _metaFile      { $_[0]{_infoFile} ||= catfile($_[0]{path}, 'file_cas_store_simple.yml'); }

=head1 METHODS

=head2 new( path => $path )

=cut
sub new {
	my $class= shift;
	my %p= Params::Validate::validate(@_, { path => 1, alg => 0, create => 0, ignoreVersion => 0 });
	my $create= delete $p{create};
	my $ignoreVersion= delete $p{ignoreVersion};
	$p{path}= "$p{path}"
		if ref $p{path};
	$p{alg} ||= 'auto';
	my $self= bless \%p, $class;
	
	unless (-f $self->_metaFile) {
		croak "Path does not appear to be a CAS: '$p{path}'"
			unless $create;
		$self->{alg}= 'sha1' if $self->alg eq 'auto';
		$self->initializeStore();
	}
	
	my ($storeSettings)= LoadFile($self->_metaFile)
		or croak "Error reading store attributes from '".$self->_metaFile."'";
	
	$self->{alg}= $storeSettings->{algorithm}
		if $self->alg eq 'auto';
	$self->{hashOfNull}= $self->_newHash->hexdigest();
	
	# Sanity checks
	croak "Hash algorithm mismatch: $storeSettings->{algorithm} != $self->{alg}"
		unless $storeSettings->{algorithm} eq $self->alg;
	croak "Store was created by a newer version of this module.  Pass 'ignoreVersion=>1' if you want to try anyway."
		unless $ignoreVersion or $storeSettings->{VERSION} <= $VERSION;
	croak "Store is missing a required entry (indicating a possibly corrupt tree)"
		unless $self->get($self->hashOfNull);
	
	return $self;
}

sub initializeStore {
	my ($self)= @_;
	make_path($self->path);
	my $info= {
		VERSION => $VERSION,
		algorithm => $self->alg,
	};
	DumpFile($self->_metaFile, $info);
	$self->put('');
}

=head2 get

This does NOT return the data bytes associated with the hash.

It returns a hashref of metadata which you can pass to ->read() to read your
data into a buffer.  This allows us to copy with files larger than memory.

Two guaranteed fields are 'hash' (a copy of the parameter to this method)
and 'size', which is the total size in bytes of the data referenced by this
hash.

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

Note that instances of Path::Class::* are handled by the File::CAS module, not the store.

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
		($destFh, $fname)= tempfile( 'temp-XXXXXXXX', DIR => $self->path );
	}
	binmode $destFh;
	
	try {
		my $alg= $self->_newHash unless defined $hash;
		# simple scalar
		if (!ref $data) {
			$alg->add($data) unless defined $hash;
			_writeAllOrDie($destFh, $data);
		}
		# else we read from the supplied file handle
		else {
			my $buf;
			while(1) {
				my $got= sysread($data, $buf, ($self->copyBufferSize || 256*1024));
				if ($got) {
					# hash it (maybe)
					$alg->add($buf) unless defined $hash;
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
			$hash= $alg->hexdigest;
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
	my $alg= $self->_newHash;
	$alg->add($data) unless ref $data;
	$alg->addfile($data) if ref $data;
	$alg->hexdigest;
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

=head2 sweep( $setOfHashes, $confirmProc )

Remove all hashes which are not in the given set.  

This is a dangerous operation, and you should make sure you have all the
hashes you want to keep listed in the set.

If $confirmProc is given, it will be passed the name of every hash which
is about to be deleted.  If it returns true, the entity will indeed be
removed.  If it returns false, nothing happens and the algorithm continues.
If it returns undef, the algorithm is ended.

$confirmProc provides a good way to list all the deleted entities, or to
perform a "dry run" of the sweep.

=cut
sub sweep {
	my ($self, $setOfHashes, $confirmProc)= @_;
	# TODO:
	croak "Unimplemented";
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
	Digest::SHA->new($_[0]{alg});
}

sub _pathForHash {
	my ($self, $hash)= @_;
	catfile( $self->path, substr($hash, 0, 2), substr($hash, 2, 2) ), substr($hash,4);
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
