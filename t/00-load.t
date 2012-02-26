#!perl -T

use Test::More;

BEGIN {
	use_ok $_ or BAIL_OUT('use $_')
		for qw(
			File::CAS::File
			File::CAS::Dir
			File::CAS::Scanner
			File::CAS::Store::Simple
			File::CAS
		);
}

diag( "Testing File::CAS $File::CAS::VERSION, Perl $], $^X" );
done_testing;