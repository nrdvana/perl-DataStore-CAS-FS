#!perl -T
use strict;
use warnings;

use Test::More;

use_ok('File::CAS::Dir') || BAIL_OUT;
use_ok('File::CAS::Store::Virtual') || BAIL_OUT;

my %metadata= (
	foo => 1,
	bar => 2,
	baz => 3
);
my @entries= (
	{ type => 'file',     name => 'a', size => 10, hash => '0000' },
	{ type => 'file',     name => 'b', size => 10, hash => '1111', symlinkTarget => 'abcdef' },
	{ type => 'symlink',  name => 'c', size => 10, hash => '2222', symlinkTarget => 'fedcba' },
	{ type => 'blockdev', name => 'd', size => 10000, hash => '3333', symlinkTarget => '', device => '1234' },
	{ type => 'chardev',  name => 'e', size => 0, hash => undef, device => '4321' },
	{ type => 'pipe',     name => 'f', size => 1, hash => 'dfljsdlfkj' },
	{ type => 'socket',   name => 'g', size => 1, hash => 'sfsdfsdf' },
);

my $sto= new_ok('File::CAS::Store::Virtual', [], 'create temp store');

my $ser= File::CAS::Dir->SerializeEntries(\@entries, \%metadata);
ok( length($ser) > 50, 'serialized soemthing' );

ok( my $hash= $sto->put($ser), 'stored dir' );
isa_ok( my $file= $sto->get($hash), 'File::CAS::File', 'file object for dir' );
my $dir= new_ok('File::CAS::Dir', [ $file ], 'created dir object');

is_deeply( $dir->{_metadata}, \%metadata, 'deserialized metadata' );
is_deeply( [ map { $_->asHash } @{$dir->{_entries}} ], \@entries, 'deserialized entries' );

done_testing;