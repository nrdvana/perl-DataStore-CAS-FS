#! /usr/bin/env perl
use strict;
use warnings;

use Test::More;
use Path::Class;
use Data::Dumper;
use Try::Tiny;

use_ok('File::CAS::Store::Simple') || BAIL_OUT;

chdir('t') if -d 't';
-d 'cas_tmp' or BAIL_OUT('missing cas_tmp directory for testing file-based cas');

dir('./cas_tmp/cas_store_simple')->rmtree(0, 0);
ok( ! -d './cas_tmp/cas_store_simple' );

my $sto= new_ok('File::CAS::Store::Simple', [ path => './cas_tmp/cas_store_simple', create => 1 ]);

# all stores should contain the epty string
my $empty;
ok( $empty= $sto->get($sto->hashOfNull), 'lookup of empty value' );

# write the empty string
is( $sto->put(''), $sto->hashOfNull, 'store empty value' );
is( $sto->put(''), $sto->hashOfNull, 'store empty value again' );

# read the empty string
my $buffer= "foo";
is($sto->readFile($empty, $buffer, 1024), 0, 'read empty file');
is($!, '', 'no error');
is($buffer, '', 'got empty buffer');
is($sto->closeFile($empty), 1);

# see if the self-check works
ok($sto->validate($empty->{hash}), 'validate');

# write a longer string
my $info;
my $str= "A string of text!\nAnd another line of text.\n";
my $hash= $sto->calcHash($str);
ok(!defined ($info= $sto->get($hash)), 'hash not stored yet');
is($sto->put($str), $hash, 'hash stored');
ok(($info= $sto->get($hash)), 'hash stored now');

# read the string back out
is($sto->readFile($info, $buffer, 1024), length($str), 'read full string');
is($buffer, $str, 'identical data');
is($sto->readFile($info, $buffer, 1024), 0, 'correct eof');

# see if the self-check works
ok($sto->validate($info->{hash}), 'validate');

# now create a new store on the same path
my $sto2= new_ok('File::CAS::Store::Simple', [ path => './cas_tmp/cas_store_simple' ]);
is($sto2->get($hash)->{size}, length($str), 'found same string');

done_testing;