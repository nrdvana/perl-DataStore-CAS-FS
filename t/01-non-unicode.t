#!perl 
use strict;
use warnings;
use Test::More;
use Data::Dumper;
use JSON;

use_ok('DataStore::CAS::FS::NonUnicode') || BAIL_OUT;

my $j= JSON->new()->convert_blessed
	->filter_json_single_key_object(
		'*NonUnicode*' => \&DataStore::CAS::FS::NonUnicode::FROM_JSON
	);

my $x= DataStore::CAS::FS::NonUnicode->new("\x{FF}");
my $json= $j->encode($x);
my $x2= "".$j->decode($json);
is( $x, $x2 );
ok( !utf8::is_utf8($x2) );
ok( ref $x2 );
$x2= "$x2";
ok( !ref $x2 );

done_testing;