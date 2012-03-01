package File::CAS::Store;
use strict;
use warnings;
use Carp;

sub new {
	my $class= shift;
	$class->_ctor({ ref($_[1])? %{$_[1]} : @_ });
}

sub _ctor_params {  }

# sub _ctor {}

sub hashOfNull {
	my $self= shift;
	$self->{hashOfNull}= $self->calchash('') unless defined $self->{hashOfNull};
	$self->{hashOfNull};
}

# sub get

# sub put

# sub calcHash

# sub validate

# sub sweep

# sub readFile

# sub seekFile

# sub closeFile


1;