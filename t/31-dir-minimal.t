#!perl -T
use strict;
use warnings;

use Test::More;

use_ok('File::CAS::Dir::Minimal') || BAIL_OUT;
use_ok('File::CAS::Store::Virtual') || BAIL_OUT;

my %metadata= (
	foo => 1,
	bar => 2,
	baz => 3
);
my @entries= (
	{ type => 'file',     name => 'a', size => 10, hash => '0000' },
	{ type => 'file',     name => 'b', size => 10, hash => '1111', linkTarget => 'abcdef' },
	{ type => 'symlink',  name => 'c', size => 10, hash => '2222', linkTarget => 'fedcba' },
	{ type => 'blockdev', name => 'd', size => 10000, hash => '3333', linkTarget => '', device => '1234' },
	{ type => 'chardev',  name => 'e', size => 0, hash => undef, device => '4321' },
	{ type => 'pipe',     name => 'f', size => 1, hash => 'dfljsdlfkj' },
	{ type => 'socket',   name => 'g', size => 1, hash => 'sfsdfsdf' },
);
my @expected= (
	{ type => 'file',     name => 'a', hash => '0000', size => undef, linkTarget => undef, device => undef },
	{ type => 'file',     name => 'b', hash => '1111', size => undef, linkTarget => undef, device => undef },
	{ type => 'symlink',  name => 'c', hash => undef, size => undef, linkTarget => 'fedcba', device => undef },
	{ type => 'blockdev', name => 'd', hash => undef, size => undef, linkTarget => undef, device => '1234' },
	{ type => 'chardev',  name => 'e', hash => undef, size => undef, linkTarget => undef, device => '4321' },
	{ type => 'pipe',     name => 'f', hash => undef, size => undef, linkTarget => undef, device => undef },
	{ type => 'socket',   name => 'g', hash => undef, size => undef, linkTarget => undef, device => undef },
);

my $sto= new_ok('File::CAS::Store::Virtual', [], 'create temp store');

my $ser= File::CAS::Dir::Minimal->SerializeEntries(\@entries, \%metadata);
ok( length($ser) > 50, 'serialized soemthing' );

ok( my $hash= $sto->put($ser), 'stored dir' );
isa_ok( my $file= $sto->get($hash), 'File::CAS::File', 'file object for dir' );
isa_ok( my $dir= File::CAS::Dir->new($file), 'File::CAS::Dir::Minimal', 'created dir object');

for my $e (@expected) {
	ok( my $entry= $dir->find($e->{name}), "find entry $e->{name}" );
	for my $k (keys %$e) {
		is( $entry->$k(), $e->{$k}, "match field $k" );
	}
}

done_testing;