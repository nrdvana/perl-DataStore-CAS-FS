#! /usr/bin/env perl -T
use strict;
use warnings;
use Test::More;

use_ok( 'DataStore::CAS::FS::Scanner' ) || BAIL_OUT;

my $scn= new_ok( 'DataStore::CAS::FS::Scanner', [] );

chdir('t') if -d 't';
-d 'scantest1' or BAIL_OUT('missing scantest1 directory for testing directory scanner');
-d 'scantest2' or BAIL_OUT('missing scantest1 directory for testing directory scanner');

ok( my $dir= $scn->scan_dir('scantest1'), 'scan' );
my @expected= (
	[ C    => file => 2 ],
	[ a    => file => 0 ],
	[ b    => file => 0 ],
	[ c    => file => 0 ],
	[ d    => dir  => undef ],
	[ ee   => file => 5 ],
	[ f    => dir  => undef ],
	[ link => symlink => 2 ],
);

my $entries= $dir->{entries};

is(scalar(@$entries), scalar(@expected), 'correct count');
for (my $i=0; $i < @expected; $i++) {
	my ($name, $type, $size)= @{$expected[$i]};
	is($entries->[$i]{name}, $name, 'name');
	is($entries->[$i]{type}, $type, 'type');
	is($entries->[$i]{size}, $size, 'size');
	is($entries->[$i]{modify_ts}, (lstat "scantest1/$name")[9], 'mtime');
}

# unicode tests ------------------------

@expected= (
	"F\x{C3}\x{9C}BAR",
	"\x{E8}\x{A9}\x{A6}\x{E3}\x{81}\x{97}",
);

ok( $dir= $scn->scan_dir('scantest2'), 'scan utf-8 as bytes' );
$entries= $dir->{entries};
for (my $i= 0; $i < @expected; $i++) {
	is( $entries->[$i]{name}, $expected[$i], 'name' );
}


done_testing;