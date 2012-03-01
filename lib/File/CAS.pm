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
use File::CAS::Scanner;

=head1 SYNOPSIS

File::CAS is simply the interface to other backends.
All backends inherit from this package.

You can use File::CAS->new as a shortcut to loading
the appropriate backend and creating instances of it.

  use File::CAS;
  my $cas= File::CAS->new(engine => 'File', path => '/mnt/usb_external');

=head1 ATTRIBUTES

=head2 store - read-only

An instance of 'File::CAS::Store' or a subclass.

=head2 scanner - read/write

An instance of File::CAS::Scanner, or subclass.  It is responsible for
scanning real filesystem directories during "putDir".  If you didn't
specify one in the constructor, one will be created automatically.

You may alter this instance, or specify an alternate one during the
constructor, or overwrite this attribute with a new object reference.

=cut

sub store { $_[0]{store} }

sub scanner { $_[0]{scanner}= $_[1] if (scalar(@_)>1); $_[0]{scanner} }

=head1 METHODS

=head2 new( %args | \%args )

Parameters:

=over

=item store - required

It may be a class name like 'Simple' which
refers to the namespace File::CAS::Store::, or it may be a fully constructed
instance.  If it is a class name, you may also specify parameters for that
class in-line with the rest of the CAS parameters, and they will be sorted
out automagically.

=item scanner - optional

Allows you to specify a scanner object
which is used during "putDir" to collect metadata about the directory
entries.

=item filter - optional

Alias for scanner->filter.

=back

=cut

sub new {
	my $class= shift;
	my %p= ref($_[0])? %{$_[0]} : @_;
	
	defined $p{store} or croak "Missing required parameter 'store'";
	
	# if they gave us a store class name, we construct an instance of it
	unless (ref $p{store}) {
		my $cls= (index($p{store}, '::') >= 0)? $p{store} : 'File::CAS::Store::'.$p{store};
		$cls->can('_ctor_params')
			or eval "require $cls"
			or die "$@";
		my $storeArgs= {};
		for my $pname ($cls->_ctor_params) {
			$storeArgs->{$pname}= delete $p{$pname};
		}
		$p{store}= $cls->_ctor($storeArgs);
	}
	
	# pick a default scanner
	unless ($p{scanner}) {
		$p{scanner}= File::CAS::Scanner->new();
	}
	
	if (defined $p{filter}) {
		$p{scanner}->filter(delete $p{filter});
	}
	
	$class->_ctor(\%p);
}

sub newFromSpec {
	my ($class, $spec)= @_;
	if (-e $spec) {
		if (-f $spec && ($spec =~ /\.ya?ml$/i)) {
			require YAML;
			my $params= YAML::LoadFile($spec)
				or die "Error reading '$spec': $!";
			return $class->new($params);
		}
		elsif (-d $spec) {
			return $class->new(store => 'Simple', path => $spec);
		}
		die "Don't know how to use '$spec'\n";
	} elsif ($spec =~ /^\(.*\)$/) {
		return $class->new(
			map {
				($_ =~ /(.+)=([^=]*)$/) or die "Syntax error in spec: '$_'\n";
				($1, $2)
			} split ',', substr($spec, 1, -1)
		);
	}
	die "Unknown spec format: '$spec'\n";
}

our @_ctor_params= qw: scanner store dirClass :;
sub _ctor_params { @_ctor_params }

sub _ctor {
	my ($class, $params)= @_;
	my $p= { map { $_ => delete $params->{$_} } @_ctor_params };
	croak "Invalid parameter: ".join(', ', keys %$params)
		if (keys %$params);
	bless $p, $class;
}

sub get {
	# my ($self, $hash)= @_;
	$_[0]{store}->get($_[1]);
}

sub getDir {
	# my ($self, $hash)= @_;
	return File::CAS::Dir->new($_[0]{store}->get($_[1]));
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
	return $_[0]->putScalar($_[1]) unless ref $_[1];
	return $_[0]->putDir($_[1])    if ref($_[1])->isa('Path::Class::Dir');
	return $_[0]->putFile($_[1])   if ref($_[1])->isa('Path::Class::File');
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
	open(my $fh, '<:raw', "$fname")
		or croak "Can't open '$fname': $!";
	$self->{store}->put($fh);
}

sub putDir {
	my ($self, $dir, $dirHint)= @_;
	$self->scanner->storeDir($self, $dir, $dirHint);
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
