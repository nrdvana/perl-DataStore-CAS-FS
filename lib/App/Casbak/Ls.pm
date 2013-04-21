package App::Casbak;
use strict;
use warnings;

# NOTE: The current package is set to App::Casbak
#       These methods will show up in the main Casbak class.

sub ls {
	my ($self, $params)= @_;
	my $prevRootSpec= '\0';
	my $root;
	Trace('Casbak->ls(): ', $params);
	for my $item (@{$params->{paths}}) {
		# If user requested a specific root, try to load it
		if (defined $item->{hash}) {
			if ($prevRootSpec ne $item->{hash}) {
				my $hash= $self->cas->findHashByPrefix($item->{hash});
				defined $hash or die "Invalid or ambiguous hash: '$item->{hash}'\n";
				$root= $self->cas->getDir($hash)
					or die "Missing root directory: #$hash\n";
				$prevRootSpec= $item->{hash};
			}
		}
		# if user requested root at a specific timestamp, try to load it
		elsif (defined $item->{date}) {
			if ($prevRootSpec ne $item->{date}) {
				my $snap= $self->getSnapshotOrDie($item->{date});
				$root= $self->cas->getDir($snap->[1])
					or die "Missing root directory: #$snap->[1]\n";
				$prevRootSpec= $item->{date};
			}
		}
		# default to "now" if no date or hash was specified
		elsif (!$root) {
			my $snap= $self->getSnapshotOrDie('0D');
			$root= $self->cas->getDir($snap->[1])
				or die "Missing root directory: #$snap->[1]\n";
			$prevRootSpec= '0D';
		}
		# Now, look up this path in the chosen root
		$self->printDirListing({ %$params, root => $root, path => $item->{path} });
	}
}

sub printDirListing {
	my ($self, $params)= @_;
	my $path= $params->{path};
	$params->{root}->isa('File::CAS::Dir')
		or die "Root is not a directory\n";
	my ($dirEnt, $dir);
	my @path= grep { defined && length } split '/', $path
		if (defined($path) && length($path));
	if (@path) {
		$dirEnt= $params->{root}->find(@path)
			or die "No such file or directory: '$path'\n";
		if ($dirEnt->type eq 'dir' && defined $dirEnt->hash) {
			$dir= $self->cas->getDir($dirEnt->hash);
		}
	}
	else {
		$path= '/';
		$dirEnt= File::CAS::DirEnt->new(name => '/', type => 'dir', hash => $params->{root}->hash, size => $params->{root}->size);
		$dir= $params->{root};
	}
	
	if ($dir and !$params->{directory}) {
		# list directory contents
		print "$path:\n";
		print $self->formatDirEnt($_, $params)."\n" for $dir->getEntries;
	}
	else {
		# list single entry
		print $self->formatDirEnt($dirEnt, $params)."\n";
	}
}

sub formatDirEnt {
	my ($self, $dirEnt, $params)= @_;
	return $dirEnt->name
	#	if $params->{long};
}

1;