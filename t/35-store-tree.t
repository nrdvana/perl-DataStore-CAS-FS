#!perl -T
use strict;
use warnings;

use Test::More;

use_ok('File::CAS') || BAIL_OUT;

my $scn= new_ok('File::CAS', [ store => 'File::CAS::Store::Virtual' ]);

chdir('t') if -d 't';

$scn->putDir(".");

done_testing;