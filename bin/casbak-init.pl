#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use App::Casbak;

my %init;

GetOptions(
	'version|V' => sub { print $App::Casbak::VERSION."\n"; },
	'help|?' => sub { pod2usage(-verbose => 2, -exitcode => 1) },
	'cas|C' => \$init{backupDir},
) or pod2usage(2);

for my $arg (@ARGV) {
	($arg =~ /^([A-Za-z_][A-Za-z0-9_.]*)=(.*)/)
		or pod2usage("Invalid name=value pair: '$arg'\n");
	apply(\%init, [split /\./, $1], $2, 0 );
}

defined $init{cas}{store} and length $init{cas}{store}
	or pod2usage("Parameter 'cas.store' is required\n");

App::Casbak->init(\%init);
exit 0;

sub apply {
	my ($hash, $path, $value, $i)= @_;
	my $field= $path->[$i];
	if ($i < $#$path) {
		$hash->{$field} ||= {};
		if (!ref $hash->{$field}) {
			warn "using implied ".join('.', @$path[0..$i]).".CLASS = $value\n";
			$hash->{$field}= { CLASS => $hash->{$field} };
		}
		apply($hash->{$field}, $path, $value, $i+1);
	} else {
		warn "Multiple values specified for ".join('.', @$path).".  Using '$value'.\n"
			if defined $hash->{$field};
		$hash->{$field}= $value;
	}
}

__END__
=head1 NAME

casbak-init - initialize a casbak backup directory

=head1 SYNOPSIS

casbak-init [options] store=CLASS [name=value [...]]

where each name/value pair is a valid parameter to File::CAS->new

=cut

