#!perl
use strict;
use warnings;

use Test::More;
use Path::Class;
use Data::Dumper;
use Try::Tiny;
use Carp;

$SIG{__WARN__}= sub { carp($_) };

package FakeStore;
use strict;
use warnings;
use File::CAS;

our %Data= (
	1 => do { my $x= '0123456789'; while (length $x < 80) { $x .= $x; } $x; },
	2 => do { my $x= '0123456789'; while (length $x < 1000000) { $x .= $x; } $x; },
	3 => "abcdef\nghijkl\n\n",
);

sub new { bless {}, $_[0] }

sub get {
	my ($self, $hash)= @_;
	return File::CAS::File->_ctor({ store => $self, hash => $hash, size => length($Data{$hash}) });
}

sub readFile {
	my ($self, $file, undef, $length, $offset)= @_;
	$file->{_pos}= 0 unless defined $file->{_pos};
	my $src= $Data{$file->{hash}};
	my $count= $length;
	my $remain= length($src) - $file->{_pos};
	$count= $remain if ($remain < $count);
	$_[2]= '' unless defined $_[2];
	substr($_[2], $offset)= substr($src, $file->{_pos}, $count);
	$file->{_pos} += $count;
	$count;
}

sub seekFile {
	my ($self, $file, $ofs, $whence)= @_;
	my $src= $Data{$file->{hash}};
	$file->{_pos} ||= 0;
	$ofs ||= 0;
	$whence ||= 0;
	$ofs += $file->{_pos} if $whence == 1;
	$ofs += length($src) if $whence == 2;
	$ofs= length($src) if $ofs > length($src);
	$ofs= 0 if $ofs < 0;
	($file->{_pos}= $ofs) || '0 but true';
}

sub closeFile {
	my ($self, $file)= @_;
	delete $file->{_pos};
}

package main;

my $sto= new_ok('FakeStore', [], 'Create a fake store');
my $f= $sto->get(1);

ok( defined($f), 'found hash 1' );
is( $f->size, length($FakeStore::Data{1}), 'correct length' );
is( $f->hash, '1' );
is( $f->store, $sto );

# create the filehandle
my $vfh;
isa_ok(($vfh= $f->newHandle), 'GLOB', 'file->newHandle' );

# basic read
my $buf;
is( sysread($vfh, $buf, 10), 10, 'read 10 bytes' );
is( length($buf), 10, 'got 10 bytes' );
is( tell($vfh), 10, 'at pos 10' );

# correct EOF conditions
is( sysread($vfh, $buf, 99999), length($FakeStore::Data{1})-10, 'read remaining bytes' );
is( length($buf), length($FakeStore::Data{1})-10, 'got remaining bytes' );
is( tell($vfh), length($FakeStore::Data{1}), 'at end' );
ok( eof($vfh), 'eof is true' );

# readline in "slurp" context
is( seek($vfh, 0, 0), '0 but true', 'rewind' );
{ local $/= undef; $buf= <$vfh>; }
is( $buf, $FakeStore::Data{1}, 'slurp' );

# readline in scalar context
$vfh= $sto->get(3)->newHandle;
is(<$vfh>, "abcdef\n", 'readline' );

# readline on a really long string with no newlines
$vfh= $sto->get(2)->newHandle;
is(<$vfh>, $FakeStore::Data{2}, 'readline (long)' );

#readline in list context
$vfh= $sto->get(3)->newHandle;
is_deeply( [ <$vfh> ], [ "abcdef\n", "ghijkl\n", "\n" ], 'readline (array ctx)' );

done_testing;