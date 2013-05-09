#! /usr/bin/env perl -T
use strict;
use warnings;
use Try::Tiny;
use Test::More;
use Digest;

use_ok('DataStore::CAS::Virtual') || BAIL_OUT;
use_ok('DataStore::CAS::FS::DirCodec::Universal') || BAIL_OUT;

my $cas= DataStore::CAS::Virtual->new();

sub decode_utf8 { goto &DataStore::CAS::FS::InvalidUTF8::decode_utf8; }

sub dies_ok(&@) {
	my ($code, $regex, $description)= @_;
	my $err= '';
	try { $code->(); } catch { $err= $_ };
	like( $err, $regex, $description );
}

sub dir_encode {
	my ($entries, $meta)= @_;
	$meta ||= {};
	return DataStore::CAS::FS::DirCodec::Universal->encode($entries, $meta);
}

subtest empty_dir => sub {
	my $hash= DataStore::CAS::FS::DirCodec->put($cas, 'universal', [], {});
	my $file= $cas->get($hash);
	is( $file->data, qq|CAS_Dir 09 universal\n{"metadata":{},\n "entries":[\n]}|, 'encode' );
	isa_ok( my $decoded= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );
	is( $decoded->iterator->(), undef, 'zero entries' );
	
	done_testing;
};

subtest one_dirent => sub {
	my @entries= (
		{ type => 'file', name => 'test' }
	);
	my $hash= DataStore::CAS::FS::DirCodec->put($cas, 'universal', \@entries, {});
	my $file= $cas->get($hash);
	my $expected= qq|CAS_Dir 09 universal\n{"metadata":{},\n "entries":[\n{"name":"test","type":"file"}\n]}|;
	is( $file->data, $expected, 'encode' );

	isa_ok( my $dir= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );

	my $iter= $dir->iterator;
	for (@entries) {
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
		{ type => 'file',     name => 'a',       size => 10,    ref => '0000',   foo => 42, sdlfjskldf => 'sldfjhlsdkfjh' },
		{ type => 'pipe',     name => 'f',       size => 1,     ref => undef,    bar => 'xyz' },
		{ type => 'blockdev', name => 'd',       size => 10000, ref => '1234',   },
		{ type => 'file',     name => 'b',       size => 10,    ref => '1111',   1 => 2, 3 => 4, 5 => 6},
		{ type => 'chardev',  name => 'e',       size => 0,     ref => '4321',   },
		{ type => 'symlink',  name => 'c',       size => 10,    ref => 'fedcba', },
		{ type => 'socket',   name => 'g',       size => 1,     ref => undef,    },
	);
	my @expected= (
		{ type => 'file',     name => 'a',       size => 10,    ref => '0000',   foo => 42, sdlfjskldf => 'sldfjhlsdkfjh' },
		{ type => 'file',     name => 'b',       size => 10,    ref => '1111',   1 => 2, 3 => 4, 5 => 6},
		{ type => 'symlink',  name => 'c',       size => 10,    ref => 'fedcba', },
		{ type => 'blockdev', name => 'd',       size => 10000, ref => '1234',   },
		{ type => 'chardev',  name => 'e',       size => 0,     ref => '4321',   },
		{ type => 'pipe',     name => 'f',       size => 1,     ref => undef,    bar => 'xyz' },
		{ type => 'socket',   name => 'g',       size => 1,     ref => undef,    },
	);

	ok( my $hash= DataStore::CAS::FS::DirCodec->put($cas, 'universal', \@entries, {}), 'encode' );
	my $file= $cas->get($hash);

	isa_ok( my $dir= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );

	my $iter= $dir->iterator;
	for (@expected) {
		is_deeply( $iter->()->as_hash, $_, 'entry matches' );
	}
	is( $iter->(), undef, 'and next returns undef' );
	done_testing;
};

subtest unicode => sub {
	dies_ok( sub{ dir_encode([ { name => 'x' } ]) }, qr/type/, 'dies without type' );
	dies_ok( sub{ dir_encode([ { type => 'file' } ]) }, qr/name/, 'dies without name' );

	dies_ok( sub{ dir_encode([ { type => 'file', name => "\x80" } ]) }, qr/unicode/, 'dies with latin-1 name' );
	dies_ok( sub{ dir_encode([ { type => 'file', name => "x", ref => "\x80" } ]) }, qr/unicode/, 'dies with latin-1 ref' );
	dies_ok( sub{ dir_encode([ { type => 'file', name => "x", misc => "\x80" } ]) }, qr/unicode/, 'dies with latin-1 field' );
	
	my @entries= (
		{ type => 'file', name => "\x{101}", ref => "\x{101}" },
		{ type => 'file', name => "\x{100}", ref => "\x{100}" },
		{ type => 'file', name => decode_utf8("\x80"), ref => decode_utf8("\x80") },
	);
	my @expected= (
		{ type => 'file', name => decode_utf8("\x80"), ref => decode_utf8("\x80") },
		{ type => 'file', name => "\x{100}", ref => "\x{100}" },
		{ type => 'file', name => "\x{101}", ref => "\x{101}" },
	);
	my %metadata= (
		"\x{AC00}" => "\x{0C80}"
	);
	my $expected_serialized= qq|CAS_Dir 09 universal\n|
		.qq|{"metadata":{"\xEA\xB0\x80":"\xE0\xB2\x80"},\n|
		.qq| "entries":[\n|
		.qq|{"name":{"*InvalidUTF8*":"\xC2\x80"},"ref":{"*InvalidUTF8*":"\xC2\x80"},"type":"file"},\n|
		.qq|{"name":"\xC4\x80","ref":"\xC4\x80","type":"file"},\n|
		.qq|{"name":"\xC4\x81","ref":"\xC4\x81","type":"file"}\n|
		.qq|]}|;
	my $encoded= DataStore::CAS::FS::DirCodec::Universal->encode(\@entries, \%metadata);
	ok( !utf8::is_utf8($encoded), 'encoded as bytes' );
	is( $encoded, $expected_serialized, 'encoded correctly' );
	
	isa_ok( my $dir= DataStore::CAS::FS::DirCodec::Universal->decode({ file => 0, data => $encoded }), 'DataStore::CAS::FS::Dir' );
	is_deeply( $dir->metadata, \%metadata, 'deserialized metadata are correct' );
	is_deeply( [ map { $_->as_hash } @{$dir->{_entries}} ], \@expected, 'deserialized entries are correct' );
	is( ref $dir->{_entries}[0]->name, 'DataStore::CAS::FS::InvalidUTF8' );
	done_testing;
};

done_testing;