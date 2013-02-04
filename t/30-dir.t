#!perl -T
use strict;
use warnings;

use Test::More;
use Digest;
use Data::Dumper;

use_ok('DataStore::CAS::FS::Dir') || BAIL_OUT;

my $nullDirHash= '1f3821d45fa4aae2dfe222b589d00df32c8ac8df';

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
	{ type => 'chardev',  name => 'e', size => 0,     ref => '4321',   },
	{ type => 'symlink',  name => 'c', size => 10,    ref => 'fedcba', },
	{ type => 'socket',   name => 'g', size => 1,     ref => undef,    },
);
my @expected= (
	{ type => 'file',     name => 'a', size => 10,    ref => '0000',   foo => 42, sdlfjskldf => 'sldfjhlsdkfjh' },
	{ type => 'file',     name => 'b', size => 10,    ref => '1111',   1 => 2, 3 => 4, 5 => 6},
	{ type => 'symlink',  name => 'c', size => 10,    ref => 'fedcba', },
	{ type => 'blockdev', name => 'd', size => 10000, ref => '1234',   },
	{ type => 'chardev',  name => 'e', size => 0,     ref => '4321',   },
	{ type => 'pipe',     name => 'f', size => 1,     ref => undef,    bar => 'xyz' },
	{ type => 'socket',   name => 'g', size => 1,     ref => undef,    },
);
my $hashOfSerialized= '7164cf0ecde9cbd3ef0b97fc955512155b97d2d8';

my $empty_dir= DataStore::CAS::FS::Dir->SerializeEntries([], {});
is( Digest->new('SHA-1')->add($empty_dir)->hexdigest(), $nullDirHash, 'null dir serialized correctly' )
	or diag $empty_dir;

my $file= bless { hash => $nullDirHash, size => length($empty_dir) }, 'DataStore::CAS::File';
my $dir= new_ok( 'DataStore::CAS::FS::Dir', [ file => $file, data => $empty_dir ], 'null dir deserialized' );
is_deeply( $dir->{_entries}, [], 'null dir serialized correctly' )
	and is_deeply( $dir->metadata, {}, 'null dir serialized correctly' )
	or diag Dumper($dir);

my $ser= DataStore::CAS::FS::Dir->SerializeEntries(\@entries, \%metadata);
is( Digest->new('SHA-1')->add($ser)->hexdigest(), $hashOfSerialized, 'test dir serialized correctly' )
	or diag $ser;

$file= bless { hash => $hashOfSerialized, size => length($ser) }, 'DataStore::CAS::File';
$dir= new_ok( 'DataStore::CAS::FS::Dir', [ file => $file, data => $ser ], 'test dir deserialized from scalar' );

is_deeply( $dir->metadata, \%metadata, 'deserialized metadata are correct' )
	or diag Dumper($dir->metadata);
is_deeply( [ map { $_->as_hash } @{$dir->{_entries}} ], \@expected, 'deserialized entries are correct' )
	or diag Dumper($dir->{_entries});

ok( open( my $handle, '<', \$ser ), 'open memory stream' );
$dir= new_ok( 'DataStore::CAS::FS::Dir', [ file => $file, handle => $handle ], 'test dir deserialized from handle' );

is_deeply( $dir->metadata, \%metadata, 'deserialized metadata are correct' )
	or diag Dumper($dir->metadata);
is_deeply( [ map { $_->as_hash } @{$dir->{_entries}} ], \@expected, 'deserialized entries are correct' )
	or diag Dumper($dir->{_entries});

my $iter= $dir->iterator;
for (@expected) {
	ok( !$iter->eof, 'not eof yet' );
	is( $iter->next->name, $_->{name}, 'iterator matches' );
}
ok( $iter->eof, 'eof at end' );
is( $iter->next, undef, 'and next returns undef' );

# unicode testing ----------------------

@entries= (
	{ type => 'file', name => "\xC4\x80\xC5\x90", size => '100000000000000000000000000', hash => '0000' },
);
%metadata= (
	"\x{AC00}" => "\x{0C80}"
);
my $expected= "CAS_Dir 00 \n{\"metadata\":{\"\xEA\xB0\x80\":\"\xE0\xB2\x80\"},\n \"entries\":[\n{\"hash\":\"0000\",\"name\":\"\xC4\x80\xC5\x90\",\"size\":\"100000000000000000000000000\",\"type\":\"file\"}\n]}\n";
my $unicode_dir= DataStore::CAS::FS::Dir->SerializeEntries(\@entries, \%metadata);
ok( !utf8::is_utf8($unicode_dir), 'encoded as bytes' );
is( $unicode_dir, $expected, 'encoded correctly' );
$file= bless { hash => $hashOfSerialized, size => length($unicode_dir) }, 'DataStore::CAS::File';
$dir= new_ok( 'DataStore::CAS::FS::Dir', [ file => $file, data => $unicode_dir ], 'unicode dir deserialized from scalar' );
is_deeply( $dir->metadata, \%metadata, 'deserialized metadata are correct' )
	or diag Dumper($dir->metadata);
is_deeply( [ map { $_->as_hash } @{$dir->{_entries}} ], \@entries, 'deserialized entries are correct' )
	or diag Dumper($dir->{_entries});


done_testing;