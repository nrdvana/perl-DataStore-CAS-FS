package App::Casbak;
use strict;
use warnings;

# NOTE: The current package is set to App::Casbak
#       These methods will show up in the main Casbak class.

sub export {
	my ($self, $params)= @_;
	Trace('Casbak->export(): ', $params);
	die "Unimplemented";
}

1;