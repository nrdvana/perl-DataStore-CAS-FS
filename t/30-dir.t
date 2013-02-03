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
	{ type => 'file',     name => 'a', size => 10, hash => '0000' },
	{ type => 'file',     name => 'b', size => 10, hash => '1111', path_ref => 'abcdef' },
	{ type => 'symlink',  name => 'c', size => 10, hash => '2222', path_ref => 'fedcba' },
	{ type => 'blockdev', name => 'd', size => 10000, hash => '3333', path_ref => '', device => '1234' },
	{ type => 'chardev',  name => 'e', size => 0, hash => undef, device => '4321' },
	{ type => 'pipe',     name => 'f', size => 1, hash => 'dfljsdlfkj' },
	{ type => 'socket',   name => 'g', size => 1, hash => 'sfsdfsdf' },
);
my $hashOfSerialized= '8a3185b669648e602be3eaf5267666ea9146902f';

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
is_deeply( [ map { $_->as_hash } @{$dir->{_entries}} ], \@entries, 'deserialized entries are correct' )
	or diag Dumper($dir->{_entries});

ok( open( my $handle, '<', \$ser ), 'open memory stream' );
$dir= new_ok( 'DataStore::CAS::FS::Dir', [ file => $file, handle => $handle ], 'test dir deserialized from handle' );

is_deeply( $dir->metadata, \%metadata, 'deserialized metadata are correct' )
	or diag Dumper($dir->metadata);
is_deeply( [ map { $_->as_hash } @{$dir->{_entries}} ], \@entries, 'deserialized entries are correct' )
	or diag Dumper($dir->{_entries});

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