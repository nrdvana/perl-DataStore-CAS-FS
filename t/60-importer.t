#! /usr/bin/env perl -T
use strict;
use warnings;
use Test::More;
use Path::Class 'file', 'dir';

use_ok( 'DataStore::CAS::FS::Importer' ) || BAIL_OUT;
use_ok( 'DataStore::CAS::Virtual' ) || BAIL_OUT;
use_ok( 'DataStore::CAS::FS' ) || BAIL_OUT;

my $scn= new_ok( 'DataStore::CAS::FS::Importer', [] );

chdir('t') if -d 't';
-d 'cas_tmp' or BAIL_OUT('missing cas_tmp directory for testing directory scanner');

my $tree1= dir('cas_tmp','tree1');
my $tree2= dir('cas_tmp','tree2');

$tree1->rmtree(0,0);
$tree1->mkpath();
$tree1->file('file1')->touch;
$tree1->file('file2')->touch;

subtest simple => sub {
	my $cas= DataStore::CAS::Virtual->new();
	my $fs= DataStore::CAS::FS->new(store => $cas, root => {});
	
	my $importer= DataStore::CAS::FS::Importer->new();
	ok( my $attrs= $importer->collect_dirent_metadata($tree1->file('file1')), 'scan file1' );
	is( $attrs->{name}, 'file1', 'name' );
	is( $attrs->{type}, 'file', 'type' );
	is( $attrs->{size}, 0, 'size' );
	is( $attrs->{ref}, undef, 'ref' );

	ok( my $ent= $importer->import_directory_entry($cas, $tree1->file('file1')), 'import file1' );
	is( $ent->ref, $cas->hash_of_null, 'ref' );

	done_testing;
};

done_testing;