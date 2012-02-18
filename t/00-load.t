#!perl -T

use Test::More;

BEGIN {
	use_ok $_ or BAIL_OUT('use $_')
		for qw(
			File::CAS::Store::Simple
			File::CAS::DirEntry
			File::CAS::DirScan
			File::CAS::Dir
			File::CAS
		);
}

diag( "Testing File::CAS $File::CAS::VERSION, Perl $], $^X" );
done_testing;