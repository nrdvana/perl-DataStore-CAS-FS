#! /usr/bin/env perl

use strict;
use warnings;

use File::CAS;
use File::CAS::Fuse;
use Getopt::Long 'GetOptionsFromArray';
use Pod::Usage;

=head1 NAME

mountcas

=head1 DESCRIPTION

Fuse filesystem access to a File::CAS

=head1 SYNOPSYS

  mountcas MyCas.yml 10238AB564DC14234234 /mnt/test

=head1 USAGE

  mountcas MyCas.yml 10238AB564DC14234234 /mnt/test

=cut

our $self;

my %p;
GetOptions(
	'help|h|?' => \$p{wantHelp},
	'version|v' => \$p{wantVersion},
) or pod2usage(2);

$p{cas}= shift @ARGV
	unless $p{cas};

$p{rootKey}= shift @ARGV
	unless $p{rootKey};

$p{mountpoint}= shift @ARGV
	unless $p{mountpoint};

$p{wantHelp}
	and pod2usage(1);

if ($p{wantVersion}) {
	print "File::CAS  $File::CAS::VERSION\n"
	     ."Fuse       $Fuse::VERSION\n"
	     ."Fuse API   ".Fuse::fuse_version()."\n";
	exit(0);
}
	
defined $p{$_} or die "Missing required argument '$_'\n"
	for qw( cas rootKey mountpoint );
	
# now get the parameters for the CAS, and construct it.
$p{cas}= File::CAS->newFromSpec($p{cas});
	
File::CAS::Fuse::main(\%p);

1;