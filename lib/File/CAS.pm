package File::CAS;

use 5.006;
use strict;
use warnings;

=head1 NAME

File::CAS - Content-Addressable Storage for file trees

=head1 DESCRIPTION

TODO:

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

use Carp;
use File::CAS::File;
use File::CAS::Dir;
use File::CAS::DirScan;
use File::CAS::DirEntry;

=head1 SYNOPSIS

File::CAS is simply the interface to other backends.
All backends inherit from this package.

You can use File::CAS->new as a shortcut to loading
the appropriate backend and creating instances of it.

  use File::CAS;
  my $cas= File::CAS->new(engine => 'File', path => '/mnt/usb_external');

=head1 METHODS

=head2 new

=cut

sub new {
	my $class= shift;
	my %p= ref($_[0])? %{$_[0]} : @_;
	if (my $engine= delete $p{engine}) {
		$class.= '::'.$engine;
		require $class;
	}
	$class->_ctor(\%p);
}

sub _ctor {
	croak "Invalid parameter: ".join(', ', keys %{$_[1]})
		if (keys %{$_[1]});
	bless {}, $_[0];
}

sub get {
	# my ($self, $hash)= @_;
	$_[0]->{store}->get($_[1]);
}

sub getDir {
	# my ($self, $hash)= @_;
	my $file= $_[0]->{store}->get($_[1]);
	return $file? File::CAS::Dir->Deserialize($file) : undef;
}

sub calcHash {
	my ($self, $thing)= @_;
	if (ref($thing)) {
		if (ref($thing)->isa('Path::Class::Dir')) {
			return $self->dirScanner->calcHash($self, $thing);
		} elsif (ref($thing)->isa('Path::Class::File')) {
			open my $f, '<:raw', "$thing";
			return $self->{store}->calcHash($f);
		}
	}
	$self->{store}->calcHash($thing);
}

sub put {
	goto &putScalar unless ref $_[1];
	goto &putDir    if ref($_[1])->isa('Path::Class::Dir');
	goto &putFile   if ref($_[1])->isa('Path::Class::File');
	# else assume handle
	$_[0]{store}->put($_[1]);
}

sub putScalar {
	my ($self, $scalar)= @_;
	$scalar= "$scalar" if ref $scalar;
	$self->{store}->put($scalar);
}

sub putHandle {
	$_[0]{store}->put($_[1]);
}

sub putFile {
	my ($self, $fname)= @_;
	open(my $fh, '<:raw', "$fname") == 0
		or croak "Can't open '$fname': $!";
	$self->{store}->put($fh);
}

sub putDir {
	my ($self, $dir, $filterProc)= @_;
}

sub calcHashFile {
	
}

sub sweep {
	$_[0]{store}->sweep($_[1]);
}

=head1 AUTHOR

Michael Conrad, C<< <mike at nrdvana.net> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-file-cas at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=File-CAS>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc File::CAS


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=File-CAS>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/File-CAS>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/File-CAS>

=item * Search CPAN

L<http://search.cpan.org/dist/File-CAS/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2012 Michael Conrad.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
