package File::CAS::File;
use strict;
use warnings;

=head1 NAME

File::CAS::File - File accessor object

=head1 DESCRIPTION

This class gives you access to the files (or more generically,
blobs of data, including serialized directories) within a
File::CAS store.

It provides a number of methods for performing buffered reads
on the data, and even has a ->handle method which gives you
a fancy virtual filehandle for when you don't want to pull the
entire blob into memory as a scalar.

The file object will *only* have directory entry metadata (like
name, timestamp, permissions) if you passed the "dirMeta"
attribute to the constructor, or if you used a convenience
method that did it for you.  This is because many logical files
might have the same contents, so the only way to know which
logical path you are dealing with is to have followed that path
from the root directory.

=cut

use Carp;
use Scalar::Util 'weaken';
use Symbol;

our @CARP_NOT= ('File::CAS::File', 'File::CAS::File::Handle');

=head1 ATTRIBUTES

=head2 store (mandatory, read-only)

The "store" object of the CAS which this file is located in.

=head2 hash (mandatory, read-only)

The checksum 'hash' value of the data, which is used as the key in the store.

=head2 size (mandatory, read-only)

The size, in bytes, of the hashed data.

This must always be known for every stored file.  It will be available
even if directory metadata is not known.

=head2 dirMeta (optional, read-write)

A DirEntry object describing this file as it relates to some directory
listing.  This is merely for convenience in user programs, and has no
real attachment to the file itself.

=head2 name (optional, alias, read-only)

Shortcut to dirMeta->name, if and only if dirMeta is not null.

=head2 bufPos (read-only)

A number from [0..size]  Defaults to 0.  This is the current position
of the File's stream.

=head2 bufEnd (read-only)

A number from [0..size]  Defaults to 0.  This is the position beyond the
last character int he buffer.

=head2 buffer (read-only reference, but modifiable with substr())

Direct access to the buffer.  Use this to scan the buffer for text you
are interested in extracting.

If you change the length of the buffer (not entirely recommended),
"bufPos" will be affected, but "bufEnd" remains consistent.  This preserves
the relation between "bufPos", "bufEnd", "eof" and "size", but beware that
you might not seek to where you expected if you use bufPos in your
calculation.

=head2 bufAvail (read-only)

The length of the buffer.  Alias for length($file->buffer)

=cut

sub store { $_[0]{store} }
sub hash { $_[0]{hash} }
sub size { $_[0]{size} }

sub dirMeta { $_[0]{dirMeta} }
sub name { $_[0]->dirMeta && $_[0]->dirMeta->name }

sub bufPos { $_[0]{bufEnd}-length($_[0]{buffer}); }
sub bufEnd { $_[0]{bufEnd}; }
sub buffer { $_[0]{buffer}; }
sub bufAvail { length($_[0]{buffer}) }

=head1 METHODS

=head2 $class->_ctor( \%parameters )

"_ctor" (constructor) is used as a more prive and more restricted
version of "new".  It requires a hashref, which it will directly
bless, with minimal error checking.

Mandatory parameters are:
  * store
  * hash
  * size

Any custom parameters used by subclasses or stores should begin
with underscore.

See Store::Simple for an example.

=cut
sub _ctor {
	my ($class, $p)= @_;
	defined $p->{$_} or die "Missing required attribute: $_"
		for qw: store hash size :;
	$p->{bufEnd}= 0;
	$p->{buffer}= '';
	bless $p, $class;
}

=head2 $file->newHandle

Create a new virtual filehandle for this File object.

Example:

  my $fh= $cas->get($hash)->newHandle;
  while (<$fh>) {
    print "\t$_";
  }

All file handle objects returned from this $file object act
in unison.  In other words, the $file object has the buffer
and read position, not the $handle object.

See File::CAS::File::Handle for discussion and limitations.

Implementation Note: these handles are very lightweight (just a tied
scalar ref and a new symbol), but creating them isn't free, either.
On my system, handle creation takes about 25x as long as a standard
accessor.  So, when you create one, you probably want to hold onto
it if you use it over and over rather than calling ->newhandle
each time.

I debated caching the handle in the $file object, but then the
handle needs to be a weak reference to the $file, and that would break
the above example since the $file object would get garbage-collected
immediately.

=cut
sub newHandle {
	my $fh= gensym();
	tie(*$fh, 'File::CAS::File::Handle', $_[0]);
	$fh
}

=head2 $file->growBuffer( $optionalCount )

This attempts to increase the size of the buffer by at least
$optionalCount bytes.  It returns the number of bytes added, just
like the return value of 'read', returning undef on error, and 0
on EOF.

Example (from readline):

	my ($pos, $got)= (0, 1);
	while ($got) {
		$pos= index($self->{buffer}, $/, $pos);
		return $self->consume($pos + length($/))
			if $pos >= 0;
		
		# search from the new data onward
		$pos= length($self->{buffer});
		
		# append more to the buffer
		$got= $self->growBuffer;
		defined $got or return undef;
	}
	return $self->consume;

=cut
sub growBuffer {
	my $got= $_[0]{store}->readFile(
		$_[0],                      # file object
		$_[0]{buffer},              # into the buffer
		(defined $_[1] && $_[1]>4096? $_[1] : 4096), # at least 4096 bytes
		length($_[0]{buffer})       # at the end of the buffer
	);
	return undef unless defined $got;
	croak "Unexpected end of file (pos=$_[0]{bufEnd}, size=$_[0]{size})"
		if $got == 0 && $_[0]{bufEnd} ne $_[0]{size};
	$_[0]{bufEnd}+= $got;
	$got;
}

=head2 $file->consume( $optionalCount )

This extracts N characters from the buffer, advances the file position,
and returns the string.  There *must* be enough characters in the buffer
or an error is thrown.

If the optional count is not given, it returns the entire buffer.

See the example above to see how elegant this can be.

=cut
sub consume {
	my $buf= $_[0]{buffer};
	# the optional first parameter consumes only part of the buffer
	if (defined $_[1] && $_[1] ne length($buf)) {
		croak "consume($_[1]): not enough characters in buffer (".length($buf).")"
			unless length($buf) >= $_[1];
	
		substr($buf, $_[1])= '';
		substr($_[0]{buffer}, 0, $_[1])= '';
	}
	# else we simply reset the buffer
	else {
		$_[0]{buffer}= '';
	}
	
	$buf;
}

=head2 $file->skip( $count )

Discards $count bytes from the current position, truncating the buffer or
possibly discarding it altogether and seeking further.  This is an alias
for $file->seek($count, 1);

=cut
sub skip { $_[0]->seek($_[1], 1) }

=head2 $file->tell

Gets the current position of the stream.

Alias for $file->bufPos

=cut
*tell = *bufPos;

=head2 $file->seek( $offset, $whence )

Seeks $offset bytes from the position specified by $whence.

Same semantics as seek and sysseek.  Returns the new current position
if successful.

=cut
sub seek {
	my ($self, $ofs, $whence)= @_;
	$whence ||= 0;
	$ofs ||= 0;
	$ofs += $self->bufPos if $whence == 1;
	$ofs += $self->{size} if $whence == 2;
	if ($ofs >= $self->bufPos && $ofs <= $self->{bufEnd}) {
		# no need to actually seek.  We just discard some buffer.
		substr($self->{buffer}, 0, $ofs - $self->bufPos)= '';
	} else {
		# moving beyond buffer.  Discard it and move the actual file position
		$self->{buffer}= '';
		$ofs= $self->{store}->seekFile($self, $ofs, 0)
			or return undef;
		$self->{bufEnd}= $ofs;
	}
	$ofs || '0 but true';
}

=head2 $file->eof

Returns true if the current position (->bufPos) is at the end of the file.
Note that the file may already have been fully read into the buffer, but
eof will not be true until all the data is removed from the buffer.
(using consume or read or skip or seek)

=cut
sub eof {
	$_[0]{bufEnd} >= $_[0]{size} && !length($_[0]{buffer});
}

=head2 $file->close

Calls store->closeFile, which releases any resources used for accessing the
data, and resets the file position.

Note that the File and handle object can still be used as though it were open
at position 0; you never need to explicitly open a File object.

=cut
sub close {
	$_[0]{store}->closeFile(@_);
	$_[0]{buffer}= '';
	$_[0]{bufEnd}= 0;
}

=head2 $file->read( $buffer, $count, $optionalOffset )

Just like read and sysread, this copies up to $count bytes into the
supplied buffer.  $optionalOffset can be specified to preserve a portion
of the existing buffer.

Returns the number of bytes read, 0 on EOF, or undef on error.

=cut
sub read {
	my ($self, undef, $count, $ofs)= @_;
	
	# if we're at the end of the file, we're going to return a short count
	my $avail= $self->bufAvail;
	if ($self->bufEnd >= $self->{size}) {
		unless ($avail) {
			# report EOF
			substr($_[1], $ofs)= '';
			return 0;
		}
		# else reduce count;
		$count= $avail;
	}
	
	# does some of it come from the buffer?
	if ($avail) {
		# can serve the entire read from the buffer?
		if ($avail >= $count) {
			substr($_[1], $ofs||0)= $self->consume($count);
			return $count;
		}
		
		# else we consume the entire buffer
		substr($_[1], $ofs||0)= $self->consume($avail);
		
		# and we try to append some more
		$ofs= length($_[1]);
	}
	
	# now we read directly from the store, and have exhausted the buffer
	my $got= $self->{store}->readFile($self, $_[1], $count, $ofs||0);
	$self->{bufEnd}+= $got if $got;
	return $got;
}

=head2 $file->readline

Just like readline, returns a string including the terminating $/, or
possibly a string without a terminator near EOF, or undef at EOF or
if an error occurs.

=cut
sub readline {
	my ($self)= @_;
	
	goto $self->can('slurp') unless defined $/;
	
	if (wantarray) {
		my ($line, @result);
		push(@result, $line)
			while defined($line= $self->readline);
		return @result;
	}
	
	my ($pos, $got)= (0, 1);
	while ($got) {
		$pos= index($self->{buffer}, $/, $pos);
		return $self->consume($pos + length($/))
			if $pos >= 0;
		
		# search from the new data onward
		$pos= length($self->{buffer});
		
		# append more to the buffer
		$got= $self->growBuffer;
		defined $got or return undef;
	}
	return $self->bufAvail? $self->consume : undef;
}

=head2 $file->slurp

Returns the entire contents of th file as a scalar.

This is a more convenient method of doing

  { local $/= undef; $data= $file->readline; }

=cut
sub slurp {
	my $got= 1;
	($got= $_[0]->growBuffer($_[0]{size}))
		while $got;
	return undef unless defined $got;
	return $_[0]->consume;
}

package File::CAS::File::Handle;
use strict;
use warnings;

=head1 File::CAS::File::Handle

This sub-class is used to bind all the relevant tied methods to the
methods of a File::CAS::File object.

This implementation does not currently support Perl's IO Layers,
like automatic utf8 decoding.  Consider it to always be in ':raw'
mode.  (binmode may be implemented at a future date)

The handle holds a reference to the $file object it was created from,
and you can retrieve that with

  my $file= $fh->file;

=cut

sub TIEHANDLE {
	my ($class, $file)= @_;
	#weaken $file;
	bless \$file, $class;
}

sub file { ${(shift)} }

# I'm not sure why anyone would ever want this function, but I'm adding
#  it for completeness.
sub GETC     {
	my $file= ${(shift)};
	($file->growBuffer or return undef)
		unless $file->bufAvail;
	$file->consume(1);
}

sub READ     { ${(shift)}->read(@_);     }
sub READLINE { ${(shift)}->readline(@_); }
sub SEEK     { ${(shift)}->seek(@_);     }
sub TELL     { ${(shift)}->tell;         }
sub CLOSE    { ${(shift)}->close();      }
sub EOF      { ${(shift)}->eof();        }

1;