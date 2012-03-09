#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use App::Casbak;

my %casbak= ( config => {} );
my %init;

GetOptions(
	'version|V' => sub { print $App::Casbak::VERSION."\n"; },
	'help|?' => sub { pod2usage(-verbose => 2, -exitcode => 1) },
	'cas|C' => \$casbak{backupDir},
) or pod2usage(2);

for my $arg (@ARGV) {
	($arg =~ /^([A-Za-z_][A-Za-z0-9_]*)=(.*)/)
		or pod2usage("Invalid name=value pair: '$arg'\n");
	$init{$1}= $2;
}

defined $init{store} and length $init{store}
	or pod2usage("Parameter 'store' is required\n");

App::Casbak->new(\%casbak)->init(\%init);
exit 0;

__END__
=head1 NAME

casbak-init - initialize a casbak backup directory

=head1 SYNOPSIS

casbak-init [options] store=CLASS [name=value [...]]

where each name/value pair is a valid parameter to File::CAS->new

=cut

