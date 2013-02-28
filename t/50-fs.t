#!perl -T
use strict;
use warnings;
use Carp::Always;

use Test::More;

use_ok('DataStore::CAS::Virtual') || BAIL_OUT;
use_ok('DataStore::CAS::FS') || BAIL_OUT;

my $tree= {
	f1 => 'File One',
	f2 => 'File Two',
	a => {
		f10 => 'File Ten',
		f11 => 'File Eleven',
		f12 => 'File Twelve',
		b => {
			c => {
				d => {
					L1 => \'/a/f10',
					L2 => \'/a/b',
					f1 => 'Blah Blah',
					f2 => 'Blah Blah Blah',
					e => {
						L3 => \'../../..',
						f1 => 'sldfjlsdkfj',
						L4 => \'f1',
						L5 => \'../f1',
					},
				},
			},
			f => {
				g => {
					j => {
						f1 => 'sdlfshldkjflskdfjslkdjf',
					},
				},
				h => {},
				i => \'g',
			},
		},
	},
};

# Build CAS from tree, with symbolic hashes for easy debugging
my %content;
sub _buildTree {
	my ($node, $id)= @_;
	if (ref $node eq 'HASH') {
		my @entries= map { { name => $_, _buildTree($node->{$_}, "$id.$_") } } keys %$node;
		$content{$id}= DataStore::CAS::FS::Dir->SerializeEntries(\@entries, {});
		return type => 'dir', ref => $id;
	}
	elsif (ref $node eq 'SCALAR') {
		return type => 'symlink', ref => $$node;
	}
	elsif (!ref $node) {
		$content{$id}= $node;
		return type => 'file', ref => $id;
	}
	else { die "Can't handle $node"; }
}
my $rootEntry= DataStore::CAS::FS::Dir::Entry->new( name => '', _buildTree($tree, 'root') );

my $sto= new_ok('DataStore::CAS::Virtual', [ entries => \%content ], 'create temp store');
my $cas= new_ok('DataStore::CAS::FS', [ store => $sto, root => $rootEntry ], 'create virtual cas' );

is( $cas->resolve_path('/')->[-1], $rootEntry, 'resolve root abs' );
is( $cas->resolve_path('.')->[-1], $rootEntry, 'resolve current dir at root' );

is( $cas->resolve_path('/a/b/c')->[-1]->ref, 'root.a.b.c', 'follow subdir abs' );
is( $cas->resolve_path('a/b/c')->[-1]->ref, 'root.a.b.c', 'follow subdir rel' );

is( $cas->resolve_path('/f1')->[-1]->ref,   'root.f1',    'resolve file at root abs' );
is( $cas->resolve_path('f1')->[-1]->ref,    'root.f1',    'resolve file at root rel' );
is( $cas->resolve_path('a/f10')->[-1]->ref, 'root.a.f10', 'resolve file in dir' );

is( $cas->resolve_path('a/b/f/g')->[-1]->ref, 'root.a.b.f.g', 'resolve leaf dir' );

is( $cas->resolve_path('a/b/c/d/e/L4')->[-1]->name, 'L4', 'resolve symlink which points to file' );
is( $cas->resolve_path('a/b/f/i')->[-1]->name,      'i',  'resolve symlink which points to dir' );
is( $cas->resolve_path('a/b/f/i/')->[-1]->ref,     'root.a.b.f.g',      'resolve symlink target dir' );
is( $cas->resolve_path('a/b/f/i/j/f1')->[-1]->ref, 'root.a.b.f.g.j.f1', 'resolve through symlink' );

is( $cas->resolve_path('a/../a/../a/..')->[-1], $rootEntry, 'follow ".."' );
is( $cas->resolve_path('a/./b/./././.')->[-1]->ref, 'root.a.b', 'follow "."' );
is( $cas->resolve_path('a/b/c/d/e/L3/../f10')->[-1]->ref, 'root.a.f10', 'follow symlink through ".."' );

done_testing;