package App::Casbak;
use strict;
use warnings;

# NOTE: The current package is set to App::Casbak
#       These methods will show up in the main Casbak class.

sub importFile {
	my ($self, $params)= @_;
	Trace('Casbak->import(): ', $params);
	require DateTime;
	for my $path (@{$params->{paths}}) {
		my ($srcPath, $dstPath)= ($path->{real}, $path->{virt});
		my $srcEnt= $self->cas->scanner->scanDirEnt($srcPath);
		if ($dstPath ne File::Spec->rootdir) {
			die "TODO: support non-root paths\n";
		}
		
		if ($srcEnt->{type} eq 'dir') {
			my $hash= $self->cas->putDir($srcPath);
			my $now= DateTime->now;
			$self->addSnapshot($now, $hash);
		} else {
			$dstPath ne File::Spec->rootdir
				or die "Only directories can be stored as the root directory\n";
		}
	}
}

1;
