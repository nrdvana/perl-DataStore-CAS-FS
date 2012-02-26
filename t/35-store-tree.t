#!perl -T
use strict;
use warnings;

use Test::More;

use_ok('File::CAS') || BAIL_OUT;

my $scn= new_ok('File::CAS', [ store => 'Virtual' ]);

chdir('t') if -d 't';

$scn->putDir(".");

done_testing;