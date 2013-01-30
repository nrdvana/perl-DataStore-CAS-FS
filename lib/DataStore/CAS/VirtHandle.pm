package File::CAS::File;
use strict;
use warnings;

=head1 NAME

DataStore::CAS::File - File accessor object

=head1 DESCRIPTION

These objects implement a user-friendly buffering layer on top of the store's
_file_sysread and _file_sysseek methods.  This buffering mechanism doesn't
exactly match the API of IO::Handle, but there is a method "->handle" which
returns a tied file handle that does.

If you wish to bypass the buffering mechanism, you can use "->sysread" and
"->sysseek", but be careful not to mix those calls with buffered ones.

=cut

use Carp;
use Scalar::Util 'weaken';
use Symbol;

our @CARP_NOT= ('DataStore::CAS::File', 'DataStore::CAS::File::Handle');

=head1 ATTRIBUTES

=head2 store (mandatory, read-only)

The "store" object of the CAS which this file is located in.

=head2 hash (mandatory, read-only)

The store's digest's 'hash' value of the data in this file.

=head2 size (mandatory, read-only)

The size, in bytes, of the file's data.

This must always be known for every stored file.

=head2 buf_pos (read-only)

A number from [0..size]  Defaults to 0.  This is the current position
of the File's stream.

=head2 buf_end (read-only)

A number from [0..size]  Defaults to 0.  This is the position beyond the
last character in the buffer.

=head2 buffer (read-only reference, but modifiable with substr())

Direct access to the buffer.  Use this to scan the buffer for text you
are interested in extracting.

If you change the length of the buffer (not entirely recommended),
"buf_pos" will be affected, but "buf_end" remains consistent.
This preserves the relation between "buf_pos", "buf_end", "eof" and "size",
but beware that you might not seek to where you expected if you use buf_pos
in your calculation.

=head2 buf_avail (read-only)

The number of available bytes in the buffer, beyond buf_pos.

=cut

sub store { $_[0]{store} }
sub hash { $_[0]{hash} }
sub size { $_[0]{size} }

sub buf_pos { $_[0]{buf_end}-length($_[0]{buffer}); }
sub buf_end { $_[0]{buf_end}; }
sub buffer { $_[0]{buffer}; }
sub buf_avail { length($_[0]{buffer}) }

=head1 METHODS

=head2 $class->_ctor( \%parameters )

"_ctor" (constructor) is used as a more private and more restricted
version of "new".  It requires a hashref, which it will directly
bless, with minimal error checking.

Mandatory parameters are:
  * store
  * hash
  * size

Any custom parameters used by subclasses or stores should begin
with underscore.

See DataStore::CAS::Simple for an example.

=cut
sub _ctor {
	my ($class, $p)= @_;
	defined $p->{$_} or die "Missing required attribute: $_"
		for qw: store hash size :;
	$p->{buf_end}= 0;
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

it also checks to make sure that the stream reports EOF at the same
byte offset as expected from $file->size, and throws an exception if
it doesn't match.

Example (slurp implementation):

	sub slurp {
		my $got= 1;
		($got= $_[0]->growBuffer($_[0]{size}))
			while $got;
		return undef unless defined $got;
		my $buf= $_[0]{buffer};
		$_[0]{buffer}= '';
		$buf;
	}

=cut
sub growBuffer {
	my $got= $_[0]{store}->readFile(
		$_[0],                      # file object
		$_[0]{buffer},              # into the buffer
		(defined $_[1] && $_[1]>4096? $_[1] : 4096), # at least 4096 bytes
		length($_[0]{buffer})       # at the end of the buffer
	);
	return undef unless defined $got;
	
	# This check is purely for validation of the store.
	# If the OS tells us the file has ended normally, and the store tells us
	#  that it should be longer, we die loudly.
	die "Error: Unexpected end of file in store data! (eof_pos=$_[0]{buf_end}, size=$_[0]{size}, hash=$_[0]{hash})\n"
		if $got == 0 && $_[0]{buf_end} ne $_[0]{size};
	
	$_[0]{buf_end}+= $got;
	$got;
}

=head2 $file->requireBuffer($count)

This is a quick convenience method that calls growBuffer repeatedly
until it reaches a desired size.  If the desired size cannot be reached,
it throws an exception "unexpected end of stream".

=cut
sub requireBuffer {
	my $ret;
	($ret= $_[0]->growBuffer($_[1] - length($_[0]{buffer}))) or croak("unexpected end of stream".(defined $ret? '' : $!))
		while length($_[0]{buffer}) < $_[1];
	1;
}

=head2 $file->skip( $count )

Discards $count bytes from the current position, truncating the buffer or
possibly discarding it altogether and seeking further.  This is an alias
for $file->seek($count, 1);

=cut
sub skip { $_[0]->seek($_[1], 1) }

=head2 $file->tell

Gets the current position of the stream.

Alias for $file->buf_pos

=cut
*tell = *buf_pos;

=head2 $file->seek( $offset, $whence )

Seeks $offset bytes from the position specified by $whence.

Same semantics as seek and sysseek.  Returns the new current position
if successful.

=cut
sub seek {
	my ($self, $ofs, $whence)= @_;
	$whence ||= 0;
	$ofs ||= 0;
	$ofs += $self->buf_pos if $whence == 1;
	$ofs += $self->{size} if $whence == 2;
	if ($ofs >= $self->buf_pos && $ofs <= $self->{buf_end}) {
		# no need to actually seek.  We just discard some buffer.
		substr($self->{buffer}, 0, $ofs - $self->buf_pos)= '';
	} else {
		# moving beyond buffer.  Discard it and move the actual file position
		$self->{buffer}= '';
		$ofs= $self->{store}->seekFile($self, $ofs, 0)
			or return undef;
		$self->{buf_end}= $ofs;
	}
	$ofs || '0 but true';
}

=head2 $file->eof

Returns true if the current position (->buf_pos) is at the end of the file.
Note that the file may already have been fully read into the buffer, but
eof will not be true until all the data is removed from the buffer.
(using read or readall or skip or seek)

=cut
sub eof {
	$_[0]{buf_end} >= $_[0]{size} && !length($_[0]{buffer});
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
	$_[0]{buf_end}= 0;
}

=head2 $file->read( $buffer, $count, $optionalOffset )

Just like read and sysread, this copies up to $count bytes into the
supplied buffer.  $optionalOffset can be specified to preserve a portion
of the existing buffer.

Returns the number of bytes read, 0 on EOF, or undef on error.

=cut
sub read {
	my ($self, undef, $count, $ofs)= @_;
	if (!$count) {
		defined $_[1] or $_[1]= '';
		substr($_[1], $ofs||0)= '';
		return 0;
	}
	
	# if it's a small read, and our buffer is empty, grow the buffer
	my $avail= length $self->{buffer};
	if ($count < 1024 && !$avail) {
		$avail= $self->growBuffer($count);
		return $avail unless $avail;
	}
	
	# can it come from the buffer?
	if ($avail) {
		defined $_[1] or $_[1]= '';
		# can we serve the entire read from the buffer?
		if ($avail > $count) {
			substr($_[1], $ofs||0)= substr($self->{buffer}, 0, $count);
			substr($self->{buffer}, 0, $count)= '';
			return $count;
		}
		
		# else we consume the entire buffer
		substr($_[1], $ofs||0)= $self->{buffer};
		$self->{buffer}= '';
		return $avail;
	}
	
	# else we pull it from the store
	my $got= $self->{store}->readFile($self, $_[1], $count, $ofs||0);
	$self->{buf_end}+= $got if $got;
	return $got;
}

=head2 $file->readall( $buffer, $count, [ $offset ] )

This is like read, but all the bytes requested will be delivered to
the buffer, or an exception is thrown.

Always returns true (or throws an exception).

=cut
sub readall {
	my ($self, undef, $count, $ofs)= @_;
	if (!$count) {
		defined $_[1] or $_[1]= '';
		substr($_[1], $ofs||0)= '';
		return 0;
	}
	while ($count > 0) {
		my $ret= $self->read($_[1], $count, $ofs);
		croak("unexpected end of stream".(defined $ret? '' : $!))
			unless $ret;
		$count -= $ret;
		$ofs= length($_[1]);
	}
	$_[1];
}

=head2 $file->readline

Just like readline, returns a string including the terminating $/, or
possibly a string without a terminator near EOF, or undef at EOF or
if an error occurs.

=cut
sub readline {
	my ($self)= @_;
	my $buf;
	
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
		if ($pos >= 0) {
			$self->read($buf, $pos + length($/));
			return $buf;
		}
		
		# search from the new data onward
		$pos= length($self->{buffer});
		
		# append more to the buffer
		$got= $self->growBuffer;
		defined $got or return undef;
	}
	
	return undef unless length $self->{buffer};
	
	$buf= $self->{buffer};
	$self->{buffer}= '';
	$buf;
}

=head2 $file->slurp

Returns the entire contents of the file as a scalar.

This is a more convenient method of doing

  { local $/= undef; $data= $file->readline; }

=cut
sub slurp {
	my $got= 1;
	($got= $_[0]->growBuffer($_[0]{size}))
		while $got;
	return undef unless defined $got;
	
	# return the whole buffer
	my $buf= $_[0]{buffer};
	$_[0]{buffer}= '';
	$buf;
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
sub GETC     { my $c; ${(shift)}->read($c, 1) and $c; }

sub READ     { ${(shift)}->read(@_);     }
sub READLINE { ${(shift)}->readline(@_); }
sub SEEK     { ${(shift)}->seek(@_);     }
sub TELL     { ${(shift)}->tell;         }
sub CLOSE    { ${(shift)}->close();      }
sub EOF      { ${(shift)}->eof();        }

1;