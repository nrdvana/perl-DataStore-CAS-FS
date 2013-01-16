#!perl

use Test::More;
use Try::Tiny;
use Data::Dumper;
use Path::Class;
use Carp::Always;
use JSON;
use File::Spec;
use autodie;

use_ok $_ or BAIL_OUT("use $_")
	for qw(
		File::CAS
		File::CAS::Store::Simple
		App::Casbak
	);

chdir('t') if -d 't';
-d 'cas_tmp' or BAIL_OUT('missing cas_tmp directory for testing file-based cas');

sub cleandir {
	dir('./cas_tmp/cas_simple')->rmtree(0, 0);
	mkdir('./cas_tmp/cas_simple');
	dir('./cas_tmp/casbak')->rmtree(0, 0);
	mkdir('./cas_tmp/casbak');
}

cleandir();
my $cas_sto= new_ok( 'File::CAS::Store::Simple', [ path => './cas_tmp/cas_simple', create => 1 ], 'store' );
my $cas= new_ok( 'File::CAS', [ store => $cas_sto ], 'cas' );
my $cb1= new_ok( 'App::Casbak', [ cas => $cas, backupDir => './cas_tmp/casbak' ], 'casbak from objects' );

cleandir();
$cas_sto= new_ok( 'File::CAS::Store::Simple', [ path => './cas_tmp/cas_simple', create => 1 ], 'store' );
my $cb2= new_ok( 'App::Casbak', [ cas => { store => $cas_sto }, backupDir => './cas_tmp/casbak' ], 'casbak from mixed' );
is_deeply( $cb1->getConfig, $cb2->getConfig, 'same config as last' );

cleandir();
my $cb3= new_ok( 'App::Casbak', [ cas => { store => { path => '../cas_simple', create => 1 } }, backupDir => './cas_tmp/casbak' ], 'casbak from hash' );
is_deeply( $cb1->getConfig, $cb3->getConfig, 'same config as last' );


done_testing;
