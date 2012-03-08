#! /usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;

=head1 NAME

casbak-init - initialize a casbak backup directory

=head1 SYNOPSIS

casbak-init [options] store=CLASS [name=value [...]]

where each name/value pair is a valid parameter to File::CAS->new

=cut

pod2usage(1);
