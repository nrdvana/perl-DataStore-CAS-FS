#!perl -T
use strict;
use warnings;
use Test::More;
use Data::Dumper;

use_ok('DataStore::CAS::FS::Dir::Unix') || BAIL_OUT;
use_ok('DataStore::CAS::Virtual') || BAIL_OUT;

my %metadata= (
	foo => 1,
	bar => 2,
	baz => 3
);
my @entries= (
	{ type => 'file',     name => 'a', size => 10,    ref => '0000',   foo => 42, sdlfjskldf => 'sldfjhlsdkfjh' },
	{ type => 'pipe',     name => 'f', size => 1,     ref => undef,    bar => 'xyz' },
	{ type => 'blockdev', name => 'd', size => 10000, ref => '1234',   },
	{ type => 'file',     name => 'b', size => 10,    ref => '1111',   1 => 2, 3 => 4, 5 => 6},
	{ type => 'file',     name => "\x{100}", size => 1,     ref => "\x{100}",},
	{ type => 'chardev',  name => 'e', size => 0,     ref => '4321',   },
	{ type => 'symlink',  name => 'c', size => 10,    ref => 'fedcba', },
	{ type => 'socket',   name => 'g', size => 1,     ref => undef,    },
);
my @expected= (
	{ type => 'file',     name => 'a', size => 10,    ref => '0000',   },
	{ type => 'file',     name => 'b', size => 10,    ref => '1111',   },
	{ type => 'symlink',  name => 'c', size => 10,    ref => 'fedcba', },
	{ type => 'blockdev', name => 'd', size => 10000, ref => '1234',   },
	{ type => 'chardev',  name => 'e', size => 0,     ref => '4321',   },
	{ type => 'pipe',     name => 'f', size => 1,     ref => undef,    },
	{ type => 'socket',   name => 'g', size => 1,     ref => undef,    },
	{ type => 'file',     name => "\x{C4}\x{80}", size => 1, ref => "\x{C4}\x{80}", },
);

my $sto= new_ok('DataStore::CAS::Virtual', [], 'create temp store');

my $ser= DataStore::CAS::FS::Dir::Unix->SerializeEntries(\@entries, \%metadata);
ok( length($ser) > 50, 'serialized something' );

ok( my $hash= $sto->put($ser), 'stored dir' );
isa_ok( my $file= $sto->get($hash), 'DataStore::CAS::File', 'file object for dir' );
isa_ok( my $dir= DataStore::CAS::FS::Dir->new($file), 'DataStore::CAS::FS::Dir::Unix', 'created dir object');

for my $e (@expected) {
	ok( my $entry= $dir->get_entry($e->{name}), "find entry $e->{name}" )
		or diag Dumper($dir);
	for my $k (keys %$e) {
		is( $entry->$k(), $e->{$k}, "match field $k" );
	}
}

done_testing;