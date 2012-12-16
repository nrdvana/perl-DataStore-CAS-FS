package App::Casbak;
use strict;
use warnings;

# NOTE: The current package is set to App::Casbak
#       These methods will show up in the main Casbak class.

sub importFile {
	my ($self, $params)= @_;
	Trace('Casbak->import(): ', $params);
	
	my $snapshot= $self->cas->getSnapshot();
	my $root= $snapshot? $snapshot->[1] : undef;
	
	require DateTime;
	for my $path (@{$params->{paths}}) {
		my ($srcPath, $dstPath)= ($path->{real}, $path->{virt});
		my $srcEnt= $self->cas->scanner->scanDirEnt($srcPath);
		
		$srcEnt->{type} eq 'dir'
			or $dstPath ne File::Spec->rootdir
			or croak "Source cannot be stored as '".File::Spec->rootdir."' because it is not a directory: '$srcPath'\n";
		
		my $err;
		my @dstDir= File::Spec->splitdir($dstPath);
		pop @dstDir if (@dstDir);
		my $resolvedDest= $self->cas->resolvePath($root, \@dstDir, \$err)
			or croak "Destination path does not exist in backup: '$dstPath' ($err)";
		
		my $hintDir= $self->cas->getDir($resolvedDest->[-1]->hash);
		my $hash= $self->cas->putDir($srcPath, $hintDir);
		my $newEnt= $srcEnt->asHash;
		$newEnt->{hash}= $hash;
		for (reverse @$resolvedDest) {
			$hash= $self->cas->updateDir($_, [ $newEnt ] );
			$newEnt= $_->asHash;
			$newEnt->{hash}= $hash;
		}
		$root= $newEnt;
		$root->{name}= '';
	}
	
	$self->cas->writeSnapshot($root);
}

1;
