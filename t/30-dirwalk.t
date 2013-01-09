#!perl -T
use strict;
use warnings;

use Test::More;

use_ok('File::CAS') || BAIL_OUT;
use_ok('File::CAS::Dir') || BAIL_OUT;
use_ok('File::CAS::Store::Virtual') || BAIL_OUT;

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
		$content{$id}= File::CAS::Dir->SerializeEntries(\@entries, {});
		return type => 'dir', hash => $id;
	}
	elsif (ref $node eq 'SCALAR') {
		return type => 'symlink', linkTarget => $$node;
	}
	elsif (!ref $node) {
		$content{$id}= $node;
		return type => 'file', hash => $id;
	}
	else { die "Can't handle $node"; }
}
my $rootEntry= File::CAS::Dir::Entry->new( name => '', _buildTree($tree, 'root') );

my $sto= new_ok('File::CAS::Store::Virtual', [ entries => \%content ], 'create temp store');
my $cas= new_ok('File::CAS', [ store => $sto ], 'create virtual cas' );

is( $cas->resolvePathOrDie($rootEntry, '/')->[-1], $rootEntry, 'resolve root abs' );
is( $cas->resolvePathOrDie($rootEntry, '.')->[-1], $rootEntry, 'resolve current dir at root' );

is( $cas->resolvePathOrDie($rootEntry, '/a/b/c')->[-1]->hash, 'root.a.b.c', 'follow subdir abs' );
is( $cas->resolvePathOrDie($rootEntry, 'a/b/c')->[-1]->hash, 'root.a.b.c', 'follow subdir rel' );

is( $cas->resolvePathOrDie($rootEntry, '/f1')->[-1]->hash,   'root.f1',    'resolve file at root abs' );
is( $cas->resolvePathOrDie($rootEntry, 'f1')->[-1]->hash,    'root.f1',    'resolve file at root rel' );
is( $cas->resolvePathOrDie($rootEntry, 'a/f10')->[-1]->hash, 'root.a.f10', 'resolve file in dir' );

is( $cas->resolvePathOrDie($rootEntry, 'a/b/f/g')->[-1]->hash, 'root.a.b.f.g', 'resolve leaf dir' );

is( $cas->resolvePathOrDie($rootEntry, 'a/b/c/d/e/L4')->[-1]->name, 'L4', 'resolve symlink which points to file' );
is( $cas->resolvePathOrDie($rootEntry, 'a/b/f/i')->[-1]->name,      'i',  'resolve symlink which points to dir' );
is( $cas->resolvePathOrDie($rootEntry, 'a/b/f/i/')->[-1]->hash,     'root.a.b.f.g',      'resolve symlink target dir' );
is( $cas->resolvePathOrDie($rootEntry, 'a/b/f/i/j/f1')->[-1]->hash, 'root.a.b.f.g.j.f1', 'resolve through symlink' );

is( $cas->resolvePathOrDie($rootEntry, 'a/../a/../a/..')->[-1], $rootEntry, 'follow ".."' );
is( $cas->resolvePathOrDie($rootEntry, 'a/./b/./././.')->[-1]->hash, 'root.a.b', 'follow "."' );
is( $cas->resolvePathOrDie($rootEntry, 'a/b/c/d/e/L3/../f10')->[-1]->hash, 'root.a.f10', 'follow symlink through ".."' );

done_testing;