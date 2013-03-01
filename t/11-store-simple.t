#! /usr/bin/env perl -T
use strict;
use warnings;
use Test::More;
use Try::Tiny;
use Path::Class;
use Data::Dumper;
use File::stat;

sub slurp {
	my $f= shift;
	if (ref $f ne 'GLOB') {
		open(my $handle, '<:raw', $f) or do { diag "open($_[0]): $!"; return undef; };
		$f= $handle;
	}
	local $/= undef;
	my $x= <$f>;
	return $x;
}
sub dies(&$) {
	my ($code, $comment)= @_;
	try {
		&$code;
		fail "Failed to die during '$comment'";
	}
	catch {
		ok "died - $comment";
	};
}
sub dies_like(&$$) {
	my ($code, $pattern, $comment)= @_;
	try {
		&$code;
		fail "Failed to die during '$comment'";
	}
	catch {
		like($_, $pattern, $comment);
	};
}

use_ok('DataStore::CAS::Simple') || BAIL_OUT;

chdir('t') if -d 't';
-d 'cas_tmp' or BAIL_OUT('missing cas_tmp directory for testing file-based cas');

my $casdir= dir('cas_tmp','cas_store_simple');
my $casdir2= dir('cas_tmp','cas_store_simple2');
my $casdir3= dir('cas_tmp','cas_store_simple3');

sub test_constructor {
	$casdir->rmtree(0, 0);
	mkdir($casdir) or die "$!";

	my $cas= new_ok('DataStore::CAS::Simple', [ path => $casdir, create => 1, digest => 'SHA-1', fanout => [2] ]);

	my $nullfile= $casdir->file('da','39a3ee5e6b4b0d3255bfef95601890afd80709');
	is( slurp($nullfile), '', 'null hash exists and is empty' );
	is( slurp($casdir->file('conf','fanout')), "2\n", 'fanout file correctly written' );
	is( slurp($casdir->file('conf','digest')), "SHA-1\n", 'digest file correctly written' );

	unlink $nullfile or die "$!";
	dies_like { DataStore::CAS::Simple->new(path => $casdir) } qr/missing a required/, 'missing null file';

	IO::File->new($nullfile, "w")->print("\n");
	dies_like { DataStore::CAS::Simple->new(path => $casdir) } qr/missing a required/, 'invalid null file';

	unlink $nullfile;
	unlink $casdir->file('conf','VERSION') or die "$!";
	dies_like { DataStore::CAS::Simple->new(path => $casdir) } qr/valid CAS/, 'invalid CAS dir';
	dies_like { DataStore::CAS::Simple->new(path => $casdir, create => 1) } qr/not empty/, 'can\'t create if not empty';

	$casdir->rmtree(0, 0);
	mkdir($casdir) or die "$!";
	dies_like { DataStore::CAS::Simple->new(path => $casdir, create => 1, fanout => [6]) } qr/fanout/, 'fanout too wide';

	$casdir->rmtree(0, 0);
	mkdir($casdir) or die "$!";
	dies_like { DataStore::CAS::Simple->new(path => $casdir, create => 1, fanout => [1,1,1,1,1,1]) } qr/fanout/, 'fanout too wide';

	$cas= new_ok('DataStore::CAS::Simple', [ path => $casdir, create => 1, digest => 'SHA-1', fanout => [1,1,1,1,1] ], 'create with deep fanout');
	$cas= undef;
	$cas= new_ok('DataStore::CAS::Simple', [ path => $casdir ], 're-open');
	done_testing;
}

sub test_get_put {
	$casdir->rmtree(0, 0);
	mkdir($casdir) or die "$!";

	my $cas= new_ok('DataStore::CAS::Simple', [ path => $casdir, create => 1, digest => 'SHA-1' ]);

	isa_ok( (my $file= $cas->get( 'da39a3ee5e6b4b0d3255bfef95601890afd80709' )), 'DataStore::CAS::File', 'get null file' );
	is( $file->size, 0, 'size of null is 0' );

	is( $cas->get( '0000000000000000000' ), undef, 'non-existent hash' );

	is( $cas->put(''), 'da39a3ee5e6b4b0d3255bfef95601890afd80709', 'put empty file again' );

	my $str= 'String of Text';
	my $hash= '00de5a1e6cc9c22ce07401b63f7b422c999d66e6';
	is( $cas->put($str), $hash, 'put scalar' );
	is( $cas->get($hash)->size, length($str), 'file length matches' );
	is( slurp($cas->get($hash)->open), $str, 'scalar read back correctly' );
	
	my $handle;
	open($handle, "<", \$str) or die;
	is( $cas->put($handle), $hash, 'put handle' );
	
	my $tmpfile= file('cas_tmp','test_file_1');
	$handle= $tmpfile->open('w');
	print $handle $str
		or die;
	close $handle;
	is( $cas->put($tmpfile), $hash, 'put Class::Path::File' );
	
	is( $cas->put_file("$tmpfile"), $hash, 'put_file(filename)' );
	
	is( $cas->put($cas->get($hash)), $hash, 'put DataStore::CAS::File' );
	
	done_testing;
}

sub test_hardlink_optimization {
	$casdir->rmtree(0, 0);
	$casdir2->rmtree(0, 0);
	$casdir3->rmtree(0, 0);
	mkdir($casdir) or die "$!";
	mkdir($casdir2) or die "$!";
	mkdir($casdir3) or die "$!";

	my $cas1= new_ok('DataStore::CAS::Simple', [ path => $casdir,  create => 1, digest => 'SHA-1' ]);
	my $cas2= new_ok('DataStore::CAS::Simple', [ path => $casdir2, create => 1, digest => 'SHA-1' ]);
	my $cas3= new_ok('DataStore::CAS::Simple', [ path => $casdir3, create => 1, digest => 'SHA-256' ]);

	my $str= 'Testing Testing Testing';
	my $hash1= '36803d17c40ace10c936ab493d7a957c60bdce4a';
	my $hash256= 'e6ec36e4c3abf21935f8555c5f2c9ce755d67858291408ec02328140ae1ac8b0';

	is( $cas1->put($str, { reuse_hash => 1, hardlink => 1 }), $hash1, 'correct sha-1 hash' );
	my $file= $cas1->get($hash1) or die;
	is( $file->local_file, $cas1->_path_for_hash($hash1), 'path is what we expected' );

	is( $cas2->put($file, { reuse_hash => 1, hardlink => 1 }), $hash1, 'correct sha-1 when migrated' );
	my $file2= $cas2->get($hash1) or die;
	is( $file2->local_file, $cas2->_path_for_hash($hash1) );

	my $stat1= stat( $file->local_file ) or die "stat: $!";
	my $stat2= stat( $file2->local_file ) or die "stat: $!";
	is( $stat1->dev.','.$stat1->ino, $stat2->dev.','.$stat2->ino, 'inodes match - hardlink succeeded' );

	# make sure it doesn't get the same hash when copied to a cas with different digest
	is( $cas3->put($file, { reuse_hash => 1, hardlink => 1 }), $hash256, 'correct sha-256 hash from sha-1 file' );
	my $file3= $cas3->get($hash256);
	my $stat3= stat( $file3->local_file ) or die "stat: $!";
	is( $stat3->dev.','.$stat3->ino, $stat1->dev.','.$stat1->ino, 'inodes match - hardlink succeeded' );

	is( $cas1->put($file3, { reuse_hash => 1, hardlink => 1 }), $hash1, 'correct sha-1 hash from sha-2 file' );

	done_testing;
}

subtest test_constructor => \&test_constructor;
subtest test_get_put     => \&test_get_put;
subtest test_hardlink_optimization => \&test_hardlink_optimization;

done_testing;
