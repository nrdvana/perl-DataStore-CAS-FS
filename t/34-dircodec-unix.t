#! /usr/bin/env perl -T
use strict;
use warnings;
use Test::More;

use_ok('DataStore::CAS::FS::DirCodec::Unix') || BAIL_OUT;
use_ok('DataStore::CAS::Virtual') || BAIL_OUT;

my $cas= DataStore::CAS::Virtual->new();

subtest empty_dir => sub {
	my $hash= DataStore::CAS::FS::DirCodec->put($cas, 'unix', [], {});
	my $file= $cas->get($hash);
	#is( $file->data, qq|CAS_Dir 04 unix\n\0|, 'encode' );
	isa_ok( my $decoded= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );
	is( $decoded->iterator->(), undef, 'zero entries' );
	
	done_testing;
};

subtest one_dirent => sub {
	my @entries= (
		{ type => 'file', name => 'test', ref => undef }
	);
	my @expected= (
		{ type => 'file', name => 'test' }
	);
	my $hash= DataStore::CAS::FS::DirCodec->put($cas, 'unix', \@entries, {});
	my $file= $cas->get($hash);
	#my $expected= qq|CAS_Dir 04 unix\n\0\x04\0ftest\0\0|;
	#is( $file->data, $expected, 'encode' );

	isa_ok( my $dir= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );

	my $iter= $dir->iterator;
	for (@expected) {
		is_deeply( $iter->()->as_hash, $_, 'entry matches' );
	}
	is( $iter->(), undef, 'end of list' );
	done_testing;
};

subtest many_dirent => sub {
	my %metadata= (
		foo => 1,
		bar => 2,
		baz => 3
	);
	my @entries= (
		{ type => 'file',     name => 'a', size => 10,    ref => '0000',   foo => 42, sdlfjskldf => 'sldfjhlsdkfjh' },
		{ type => 'pipe',     name => 'f', size => 1,     ref => undef,    bar => 'xyz' },
		{ type => 'blockdev', name => 'd', size => 10000, ref => '1234',   unix_uid => 5, unix_gid => 7, modify_ts => 12345678 },
		{ type => 'file',     name => 'b', size => 10,    ref => '1111',   unix_uid => 5, unix_gid => 7, 1 => 2, 3 => 4, 5 => 6},
		{ type => 'chardev',  name => 'e', size => 0,     ref => '4321',   unix_uid => 5, unix_gid => 7 },
		{ type => 'symlink',  name => 'c', size => 10,    ref => 'fedcba', unix_uid => 0, unix_blockcount => 35 },
		{ type => 'socket',   name => 'g', size => 1,     ref => undef,    },
	);
	my @expected= (
		{ type => 'file',     name => 'a', size => 10,    ref => '0000',   },
		{ type => 'file',     name => 'b', size => 10,    ref => '1111',   unix_uid => 5, unix_gid => 7 },
		{ type => 'symlink',  name => 'c', size => 10,    ref => 'fedcba', unix_uid => 0, unix_blockcount => 35 },
		{ type => 'blockdev', name => 'd', size => 10000, ref => '1234',   unix_uid => 5, unix_gid => 7, modify_ts => 12345678 },
		{ type => 'chardev',  name => 'e', size => 0,     ref => '4321',   unix_uid => 5, unix_gid => 7 },
		{ type => 'pipe',     name => 'f', size => 1,                      },
		{ type => 'socket',   name => 'g', size => 1,                      },
	);

	ok( my $hash= DataStore::CAS::FS::DirCodec->put($cas, 'unix', \@entries, {}), 'encode' );
	my $file= $cas->get($hash);

	isa_ok( my $dir= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );

	my $iter= $dir->iterator;
	for (@expected) {
		is_deeply( $iter->()->as_hash, $_, 'entry matches' );
	}
	is( $iter->(), undef, 'and next returns undef' );
	done_testing;
};

sub non_unicode { bless \$_[0], 'DataStore::CAS::FS::InvalidUTF8' }

subtest unicode => sub {
	my @entries= (
		{ type => 'file', name => "\xC4\x80\xC5\x90", size => '100000000000000000000000000', ref => '0000' },
		{ type => 'file', name => non_unicode("\x80"), size => '1', ref => "\x{C4}\x{80}" },
	);
	my @expected= (
		{ type => 'file', name => non_unicode("\x80"), size => '1', ref => "\x{C4}\x{80}" },
		{ type => 'file', name => "\xC4\x80\xC5\x90", size => '100000000000000000000000000', ref => '0000' },
	);
	my %metadata= (
		"\x{AC00}" => "\x{0C80}"
	);

	my $encoded= DataStore::CAS::FS::DirCodec::Unix->encode(\@entries, \%metadata);
	ok( !utf8::is_utf8($encoded), 'encoded as bytes' );
	
	my $file= $cas->get($cas->put_scalar($encoded));
	isa_ok( my $dir= DataStore::CAS::FS::DirCodec::Unix->decode({ file => $file }), 'DataStore::CAS::FS::Dir' );
	is_deeply( $dir->metadata, \%metadata, 'deserialized metadata are correct' );
	is_deeply( [ map { $_->as_hash } @{$dir->{_entries}} ], \@expected, 'deserialized entries are correct' );
	is( ref $dir->{_entries}[0]->name, 'DataStore::CAS::FS::InvalidUTF8' );
	done_testing;
};

done_testing;