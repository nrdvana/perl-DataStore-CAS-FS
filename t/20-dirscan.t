#!perl -T
use strict;
use warnings;

use Test::More;

use_ok('File::CAS::Scanner') || BAIL_OUT;

my $scn= new_ok('File::CAS::Scanner', []);

chdir('t') if -d 't';

ok( my $dir= $scn->scanDir('scantest1'), 'scan');
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

done_testing;