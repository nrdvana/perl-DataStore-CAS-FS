#!perl -T
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
	[ qw( C file 2 ) ],
	[ qw( a file 0 ) ],
	[ qw( b file 0 ) ],
	[ qw( c file 0 ) ],
	[ qw( d dir 0 ) ],
	[ qw( ee file 5 ) ],
	[ qw( f dir 0 ) ],
	[ qw( link symlink 2 ) ],
);

my $entries= $dir->{entries};

is(scalar(@$entries), scalar(@expected), 'correct count');
for (my $i=0; $i < @expected; $i++) {
	is($entries->[$i]{name}, $expected[$i][0], 'name');
	is($entries->[$i]{type}, $expected[$i][1], 'type');
	is($entries->[$i]{size}, $expected[$i][2], 'size');
	is($entries->[$i]{modify_ts}, (lstat "scantest1/$expected[$i][0]")[9], 'mtime');
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