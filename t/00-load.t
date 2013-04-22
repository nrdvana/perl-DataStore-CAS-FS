#! /usr/bin/env perl -T

use Test::More;

use_ok $_ or BAIL_OUT("use $_")
	for qw(
		DataStore::CAS
		DataStore::CAS::Virtual
		DataStore::CAS::Simple
		DataStore::CAS::FS
		DataStore::CAS::FS::Dir
		DataStore::CAS::FS::Scanner
		DataStore::CAS::FS::Extractor
	);

diag( "Testing DataStore::CAS $DataStore::CAS::VERSION, Perl $], $^X" );
done_testing;